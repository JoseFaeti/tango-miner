from pathlib import Path
import appdirs
import hashlib
import importlib.metadata
import importlib.util
import json
import re
import sys

from sudachipy import dictionary, tokenizer as sudachi_tokenizer

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.RunConfig import TokenizerConfig
from src.TokenizedSentence import TokenizedSentence
from src.TokenCache import TokenCache
from src.steps.ProcessingStep import ProcessingStep


RE_TAG = re.compile(r"\[(.+?)\]")
RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")
RE_ALL_DIGITS = re.compile(r"^\d+$")
RE_ALL_LATIN = re.compile(r"^[a-zA-ZÀ-ÿ\-']+$")

SKIP_POS1 = {
    "助詞", "記号", "補助記号", "感動詞", "接頭辞", "接尾辞", "代名詞",
}

SKIP_POS1_POS2 = {
    ("名詞", "固有名詞"),
    ("名詞", "代名詞"),
    ("感動詞", "フィラー"),
}

TOKENIZER_MODE = sudachi_tokenizer.Tokenizer.SplitMode.C

MIN_SENTENCE_LENGTH = 10
MAX_SENTENCE_LENGTH = 50

_tokenizer = None
_tokenizer_identity = None


# These files implement raw tokenization and cache serialization. Add any new
# project helper used by either stage to this list.
TOKENIZER_SOURCE_DEPENDENCIES = ("steps/TokenizeStep.py", "TokenCache.py")


def tokenizer_source_fingerprint(root=None) -> str:
    """Hash tokenizer and cache source, without unrelated pipeline stages."""
    root = Path(root) if root is not None else Path(__file__).resolve().parent.parent
    digest = hashlib.sha256()
    for name in TOKENIZER_SOURCE_DEPENDENCIES:
        digest.update(name.encode("utf-8") + b"\0")
        digest.update((root / name).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def tokenizer_fingerprint(settings: TokenizerConfig) -> str:
    """Hash tokenizer source, configuration, and configured dictionary contents."""
    config_path = Path(settings.config_path).expanduser().resolve()
    config_bytes = config_path.read_bytes()
    config = json.loads(config_bytes)
    choice = settings.dictionary
    if choice in ("small", "core", "full"):
        spec = importlib.util.find_spec("sudachidict_" + choice)
        if spec is None or not spec.origin:
            raise OSError(f"Cannot locate Sudachi {choice} dictionary")
        system_path = Path(spec.origin).parent / "resources" / "system.dic"
    else:
        system_path = Path(choice).expanduser()
    paths = [system_path, *(Path(p).expanduser() for p in config.get("userDict", []))]
    digest = hashlib.sha256()
    for value in (tokenizer_source_fingerprint(), sys.version, str(config_path), choice,
                  importlib.metadata.version("SudachiPy"), str(TOKENIZER_MODE)):
        digest.update(value.encode("utf-8") + b"\0")
    digest.update(config_bytes)
    for path in paths:
        path = path.resolve()
        digest.update(str(path).encode("utf-8") + b"\0")
        with path.open("rb") as f:
            while chunk := f.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def get_tokenizer(settings: TokenizerConfig | None = None, identity: str | None = None):
    global _tokenizer, _tokenizer_identity
    # No argument reuses the active tokenizer for lemma readings and legacy callers.
    if settings is None and _tokenizer is not None:
        return _tokenizer
    settings = settings if settings is not None else TokenizerConfig()
    identity = identity or tokenizer_fingerprint(settings)
    if _tokenizer is None or _tokenizer_identity != identity:
        sudachi_dictionary = dictionary.Dictionary(
            config_path=str(Path(settings.config_path).expanduser()), dict=settings.dictionary
        )
        make_tokenizer = getattr(sudachi_dictionary, "tokenizer", None)
        new_tokenizer = make_tokenizer() if make_tokenizer is not None else sudachi_dictionary.create()
        _tokenizer = new_tokenizer
        _tokenizer_identity = identity
        _lemma_reading_cache.clear()
    return _tokenizer


class RawTokenizeStep(PipelineStep):
    """Produce raw Sudachi token streams, preserving their sentence context."""

    def __init__(self, force_retokenize: bool = False, config: TokenizerConfig | None = None):
        self.config = config if config is not None else TokenizerConfig()
        self._processing_step = ProcessingStep.TOKENIZING
        self.force_retokenize = force_retokenize
        self.cache_stats = {"forced": force_retokenize, "hits": 0, "misses": 0, "disabled": 0}

    def process(self, artifact: Artifact) -> Artifact:
        files: list[tuple[Path, list[str]]] = artifact.data
        self.cache_stats.update(hits=0, misses=0, disabled=0)
        identity = tokenizer_fingerprint(self.config)
        cache = TokenCache(
            cache_dir=Path(appdirs.user_cache_dir("tango_miner")),
            tokenizer_fingerprint=identity,
        ) if artifact.tmpdir else None

        tokenized_sentences = []
        total_sentences = sum(len(sentences) for _, sentences in files)
        completed_sentences = 0

        for path, sentences in files:
            tag_match = RE_TAG.search(str(path))
            tag = tag_match.group(1) if tag_match else "[no tag]"
            tokenized_sentences.extend(
                tokenize_raw_sentences(
                    sentences, tag=tag, source_path=path, cache=cache,
                    force_retokenize=self.force_retokenize, tokenizer_config=self.config, cache_stats=self.cache_stats, tokenizer_identity=identity,
                )
            )
            completed_sentences += len(sentences)
            if completed_sentences < total_sentences:
                self.progress(completed_sentences, total_sentences, f"{path.name}")

        if cache is None:
            cache_message = "cache disabled"
        elif self.force_retokenize:
            cache_message = f"cache: 0 hits, {self.cache_stats['misses']} refreshed files"
        else:
            cache_message = (f"cache: {self.cache_stats['hits']} hits, "
                             f"{self.cache_stats['misses']} misses (files)")
        self.done(f"{len(tokenized_sentences)} raw token streams collected; {cache_message}.")
        return Artifact(tokenized_sentences)


class TokenizeStep(PipelineStep):
    """Compatibility facade for callers expecting the former combined step.

    The application pipeline uses RawTokenizeStep, MergeTokensStep, and
    BuildWordStatsStep explicitly. This facade keeps the old public contract
    for direct users while delegating to those same stages.
    """

    def __init__(self):
        self._processing_step = ProcessingStep.TOKENIZING

    def process(self, artifact: Artifact) -> Artifact:
        from src.Pipeline import Pipeline
        from src.steps.BuildWordStatsStep import BuildWordStatsStep
        from src.steps.MergeTokensStep import MergeTokensStep

        return Pipeline([
            RawTokenizeStep(),
            MergeTokensStep(),
            BuildWordStatsStep(),
        ]).run(artifact)
def tokenize_raw_sentences(
    sentences_text: list[str], *, tag: str | None, source_path: Path, cache=None,
    force_retokenize: bool = False, tokenizer_config: TokenizerConfig | None = None,
    cache_stats: dict | None = None, tokenizer_identity: str | None = None,
) -> list[TokenizedSentence]:
    """Tokenize one source file, loading/storing only raw Sudachi output."""
    all_sentence_tokens = cache.load(sentences_text) if cache and not force_retokenize else None

    if cache_stats is not None:
        key = "disabled" if not cache else "hits" if all_sentence_tokens is not None else "misses"
        cache_stats[key] += 1

    if all_sentence_tokens is None:
        if tokenizer_config is None:
            tokenizer_obj = get_tokenizer()
        elif tokenizer_identity is None:
            tokenizer_obj = get_tokenizer(tokenizer_config)
        else:
            tokenizer_obj = get_tokenizer(tokenizer_config, identity=tokenizer_identity)
        all_sentence_tokens = [
            [sudachi_node_to_dict(node) for node in tokenizer_obj.tokenize(sentence, TOKENIZER_MODE)]
            for sentence in sentences_text
        ]
        if cache:
            cache.put(sentences_text, all_sentence_tokens)

    return [
        TokenizedSentence(text=text, tag=tag, origin=source_path, tokens=tokens)
        for text, tokens in zip(sentences_text, all_sentence_tokens)
    ]


def tokenize(
    input_path,
    word_data=None,
    segmented_sentences=None,
    tag=None,
    cache=None,
    source_path=None,
    progress_handler=None,
):
    """Compatibility helper that executes the three token-processing stages."""
    if not isinstance(input_path, list):
        raise ValueError("tokenize() now only accepts list[str] sentences")

    from src.steps.BuildWordStatsStep import build_word_stats
    from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict

    origin = source_path or Path("<memory>")
    raw_sentences = tokenize_raw_sentences(
        input_path, tag=tag, source_path=origin, cache=cache
    )
    jmdict = get_jmdict()
    merged_sentences = [
        TokenizedSentence(
            text=sentence.text,
            tag=sentence.tag,
            origin=sentence.origin,
            tokens=combine_tokens_jmdict(sentence.tokens, jmdict),
        )
        for sentence in raw_sentences
    ]
    return build_word_stats(
        merged_sentences,
        progress_handler=progress_handler,
        word_data=word_data,
        segmented_sentences=segmented_sentences,
    )


# -------------------------------------------------------------------
# UTILITIES
# -------------------------------------------------------------------

_lemma_reading_cache = {}

def get_lemma_reading(lemma: str) -> str:
    if lemma in _lemma_reading_cache:
        return _lemma_reading_cache[lemma]

    m = get_tokenizer().tokenize(lemma, TOKENIZER_MODE)[0]
    reading = kata_to_hira(m.reading_form())

    _lemma_reading_cache[lemma] = reading
    return reading


def sudachi_node_to_dict(m) -> dict:
    surface = m.surface()
    lemma = m.dictionary_form() or surface
    reading = get_lemma_reading(lemma)

    return {
        "surface": surface,
        "lemma": lemma,
        "base_form": lemma,
        "reading": kata_to_hira(reading),
        "pos": m.part_of_speech(),
    }


def kata_to_hira(text: str) -> str:
    if not text:
        return ""

    result = []
    for ch in text:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            ch = chr(code - 0x60)
        result.append(ch)

    return "".join(result)


def is_useless(token: dict) -> bool:
    if token.get("grammar_reserved"):
        return True
    lemma = token["base_form"] or token["lemma"]

    if not lemma:
        return True

    if RE_ALL_DIGITS.match(lemma):
        return True

    if RE_ALL_LATIN.match(lemma):
        return True

    if not contains_japanese_script(lemma):
        return True

    if lemma[-1] in ("っ", "ッ", "ー"):
        return True

    pos1, pos2, *_ = token["pos"]

    if pos1 in SKIP_POS1 and not (lemma == 'こら' and pos1 == '感動詞'):
        return True

    if (pos1, pos2) in SKIP_POS1_POS2 and token.get('analysis_rule') != 'rise_name_boundary':
        return True

    if RE_SMALL_KANA_END.search(lemma):
        return True

    if len(lemma) == 1:
        c = lemma[0]
        if "ぁ" <= c <= "ん" or "ァ" <= c <= "ン":
            return True
    
    all_katakana = True

    for c in lemma:
        if not ("ァ" <= c <= "ン" or c == "ー"):
            all_katakana = False

    if all_katakana and not has_kanji(lemma):
        return True

    return False


def is_japanese_char(c: str) -> bool:
    return (
        "\u3040" <= c <= "\u309F"   
        or "\u30A0" <= c <= "\u30FF"  
        or "\u4E00" <= c <= "\u9FFF"   
        or "\uFF65" <= c <= "\uFF9F"   
        or c in "々〻ゝゞヽヾ"
        or c.isspace()
        or c.isdigit()
        or c.isalpha()
        or c in "（()【】「」『』…‥〜ー・"
    )


def has_kanji(text: str) -> bool:
    for c in text:
        if "一" <= c <= "龯":
            return True

    return False


def is_valid_sentence(text: str) -> bool:
    if not contains_japanese_script(text):
        return False

    if looks_like_guide_header(text):
        return False

    if text.count("[...]") > 2:
        return False

    japanese_chars = sum(1 for c in text if is_japanese_char(c))
    visible_chars = sum(1 for c in text if not c.isspace())

    if visible_chars and japanese_chars / visible_chars < 0.55:
        return False

    return MIN_SENTENCE_LENGTH <= len(text) <= MAX_SENTENCE_LENGTH


def contains_japanese_script(text: str) -> bool:
    return any(
        "\u3040" <= c <= "\u309F"  
        or "\u30A0" <= c <= "\u30FF"  
        or "\u4E00" <= c <= "\u9FFF"  
        for c in text
    )


def looks_like_guide_header(text: str) -> bool:
    if re.search(r"^[ぁ-んァ-ン一-龯]+[ \s]+[A-ZＭＳLR]([ \s]|$)", text):
        return True

    guide_labels = ("行き方", "入手方法", "戦闘開始時")
    if any(term in text for term in guide_labels) and len(text) < 20:
        return True

    return False

# Backward-compatible imports for callers of the former combined module.
from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
