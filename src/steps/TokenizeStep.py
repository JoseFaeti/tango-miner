from pathlib import Path
import appdirs
import re

from sudachipy import dictionary, tokenizer as sudachi_tokenizer
from src.JMDict import JMDict

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep
from src.SegmentedSentence import SegmentedSentence
from src.TokenCache import TokenCache
from src.WordStats import WordStats


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

RE_TAG = re.compile(r"\[(.+?)\]")
RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")
RE_ALL_DIGITS = re.compile(r"^\d+$")
RE_ALL_LATIN = re.compile(r"^[a-zA-ZÀ-ÿ\-']+$")
_KANA_RE = re.compile(r"^[\u3040-\u309F\u30A0-\u30FF]+$")

SKIP_POS1 = {
    "助詞",
    "記号",
    "補助記号",
    "感動詞",
    "接頭辞",
    "接尾辞",
    "代名詞",
}

SKIP_POS1_POS2 = {
    ("名詞", "固有名詞"),
    ("名詞", "代名詞"),
    ("感動詞", "フィラー"),
}

# List of JMDict POS categories to allow for kana-only words when matched in the token-merge phase.
# Skip things like nouns, verbs and adjectives to avoid the many false positives that would otherwise appear.
USEFUL_KANA_ONLY_POS = [
    "expression",
    "adverb (fukushi)",
    "conjunction",
    "interjection",
    "pronoun",
    "pre-noun adjectival",
]

# The fingerprint now only represents the *raw* Sudachi output.
TOKENIZER_FINGERPRINT = "2026/10/26-2"
TOKENIZER_MODE = sudachi_tokenizer.Tokenizer.SplitMode.C

MIN_SENTENCE_LENGTH = 10
MAX_SENTENCE_LENGTH = 50

_tokenizer = None
_jmdict = None


def get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = dictionary.Dictionary(
            config_path="resources/sudachi.json",
            dict="full"
        ).create()
    return _tokenizer


def get_jmdict() -> JMDict:
    """Singleton loader to ensure JMDict is only parsed into memory once."""
    global _jmdict
    if _jmdict is None:
        _jmdict = JMDict(Path.home() / "JMdict_e.xml")
    return _jmdict


# -------------------------------------------------------------------
# PIPELINE STEP
# -------------------------------------------------------------------

class TokenizeStep(PipelineStep):
    def __init__(self):
        self._processing_step = ProcessingStep.TOKENIZING


    def process(self, artifact: Artifact) -> Artifact:
        files: list[tuple[Path, list[str]]] = artifact.data

        cache = TokenCache(
            cache_dir=Path(appdirs.user_cache_dir("tango_miner")),
            tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
        ) if artifact.tmpdir else None

        self.combined_tokens = {}
        combined_sentences = []

        self.total_tokens = 0
        self.current_file = None
        self.sentence_offset = 0
        self.total_sentences = sum(len(sentences) for _, sentences in files)

        for path, sentences in files:
            self.current_file = path
            tag = RE_TAG.search(str(path))
            tag = tag.group(1) if tag else "[no tag]"

            self.combined_tokens, combined_sentences = tokenize(
                sentences,
                word_data=self.combined_tokens,
                segmented_sentences=combined_sentences,
                tag=tag,
                cache=cache,
                source_path=path,
                progress_handler=self._file_progress,
            )

            self.sentence_offset += len(sentences)
            self.total_tokens = len(self.combined_tokens)

        if cache:
            cache.flush_mtime_index()

        self.done(
            f'{self.total_tokens} tokens and {len(combined_sentences)} sentences collected.'
        )

        return Artifact(self.combined_tokens, sentences=combined_sentences)


    def _file_progress(self, current, total, message=""):
        self.progress(
            self.sentence_offset + current,
            self.total_sentences,
            f'{len(self.combined_tokens)} tokens ({self.current_file.name})',
        )


# -------------------------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------------------------

def tokenize(
    input_path,
    word_data=None,
    segmented_sentences=None,
    tag=None,
    cache=None,
    source_path=None,
    progress_handler=None
):
    if not isinstance(input_path, list):
        raise ValueError("tokenize() now only accepts list[str] sentences")

    sentences_text = input_path
    tokenizer_obj = get_tokenizer()

    word_data = word_data or {}
    segmented_out = list(segmented_sentences) if segmented_sentences else []

    # ── 1. Cache fast path (Raw Tokens) ───────────────────────────
    all_sentence_tokens = None  

    if cache and source_path:
        mtime_ns = source_path.stat().st_mtime_ns
        hash_ = cache.get_hash_by_mtime(source_path, mtime_ns)
        if hash_:
            payload = cache.load_by_hash(hash_)
            if payload:
                all_sentence_tokens = payload["tokens"]  

    # ── 2. Tokenize (only if cache missed) ────────────────────────
    if all_sentence_tokens is None:
        all_sentence_tokens = [
            [sudachi_node_to_dict(m) for m in tokenizer_obj.tokenize(s, TOKENIZER_MODE)]
            for s in sentences_text
        ]
        if cache and source_path:
            cache.put_by_mtime(
                source_path,
                mtime_ns,
                "\n".join(sentences_text),
                all_sentence_tokens,  # We cache the RAW tokens
            )

    # ── 3. Post-Processing: JMDict Compound Merger ────────────────
    # This runs every time (fast memory operation), keeping cache flexible
    jmdict = get_jmdict()
    all_sentence_tokens = [
        combine_tokens_jmdict(tokens, jmdict)
        for tokens in all_sentence_tokens
    ]

    # ── 4. Merge tokens into word_data + segmented_out ────────────
    lemma_first_pos_in_file: dict[str, int] = {}
    token_index = 0

    for i, (sentence, token_list) in enumerate(zip(sentences_text, all_sentence_tokens)):
        if progress_handler and i % 200 == 0:
            progress_handler(i, len(all_sentence_tokens))

        sentence_surfaces = {}

        for token in token_list:
            lemma = token["base_form"] or token["lemma"]
            reading = token["reading"]

            if not lemma or not reading or is_useless(token):
                continue

            token_index += 1
            if lemma not in lemma_first_pos_in_file:
                lemma_first_pos_in_file[lemma] = token_index

            sentence_surfaces[lemma] = token["surface"]

            ws = word_data.get(lemma)

            if ws:
                ws.frequency += 1
            else:
                ws = WordStats(
                    0.0,          
                    0,            
                    1,            
                    0.0,          
                    kata_to_hira(reading),
                    "",
                    set(),
                    [],
                    lemma,
                    token["pos"],
                    invalid=False,
                )
                word_data[lemma] = ws

            if tag:
                ws.tags.add(tag)

        if is_valid_sentence(sentence):
            segmented_out.append(
                SegmentedSentence(
                    text=sentence,
                    tag=tag,
                    origin=None,
                    lemma_surfaces=sentence_surfaces,
                )
            )

    # ── 5. Update index ──────────────────────────────────────────
    if token_index > 0:
        for lemma, first_pos in lemma_first_pos_in_file.items():
            ws = word_data[lemma]
            file_norm_pos = first_pos / token_index
            ws.index = (ws.index * ws.index_count + file_norm_pos) / (ws.index_count + 1)
            ws.index_count += 1

    return word_data, segmented_out


# -------------------------------------------------------------------
# TOKEN MERGER
# -------------------------------------------------------------------
def _get_field(obj, field: str, default=None):
    if isinstance(obj, dict):
        return obj.get(field, default)
    return getattr(obj, field, default)


def _has_useful_kana_only_sense(definitions: list) -> bool:
    """Return True if any sense has a useful POS for kana-only matching."""
    for definition in definitions:
        for sense in _get_field(definition, "senses", []):
            pos = _get_field(sense, "pos", [])
            if isinstance(pos, str):
                pos = [pos]

            if any(
                target in p.lower()
                for p in pos
                for target in USEFUL_KANA_ONLY_POS
            ):
                return True

    return False


def _is_prefixed_kana_noun(token_list: list[dict], definitions: list) -> bool:
    """Return True for a lexical kana noun split after a Sudachi prefix.

    Kana-only noun matches are normally too permissive: adjacent grammar tokens
    can accidentally spell a JMdict noun.  A leading ``接頭辞`` is a stronger
    morphological signal that the whole sequence is one lexical item, such as
    ``お`` + ``まもり`` -> ``おまもり``.
    """
    if len(token_list) < 2 or token_list[0]["pos"][0] != "接頭辞":
        return False

    for definition in definitions:
        for sense in _get_field(definition, "senses", []):
            pos = _get_field(sense, "pos", [])
            if isinstance(pos, str):
                pos = [pos]
            if any(p.lower().startswith("noun") for p in pos):
                return True

    return False


def combine_tokens_jmdict(
    token_list: list[dict],
    jmdict: JMDict,
    max_lookahead: int = 10,
) -> list[dict]:
    """Greedily merge adjacent lexical compounds recognized by JMdict."""
    merged_tokens = []
    n = len(token_list)
    i = 0

    while i < n:
        match_found = False

        for j in range(min(n, i + max_lookahead), i + 1, -1):
            combined_lemma = "".join(t["lemma"] for t in token_list[i:j])
            combined_surface = "".join(t["surface"] for t in token_list[i:j])
            
            token_string = combined_lemma
            definitions = jmdict.lookup_word(token_string)
            
            if not definitions:
                token_string = combined_surface
                definitions = jmdict.lookup_word(token_string)

            if not definitions:
                # no definition found
                continue

            is_prefixed_kana_noun = _is_prefixed_kana_noun(
                token_list[i:j], definitions
            )
            if (
                not has_kanji(token_string)
                and not _has_useful_kana_only_sense(definitions)
                and not is_prefixed_kana_noun
            ):
                # Most kana-only nouns are too ambiguous to merge. A noun split
                # after a Sudachi prefix is a high-confidence lexical exception.
                continue

            match_word = jmdict.get_best_entry(token_string)

            if match_word:
                # Fetch POS dynamically, falling back to "exp" (expression)
                jmdict_pos = "exp"

                if hasattr(jmdict, "get_pos"):
                    jmdict_pos = jmdict.get_pos(match_word)
                elif hasattr(match_word, "pos"):
                    jmdict_pos = match_word.pos
                
                if isinstance(jmdict_pos, str):
                    new_pos = [jmdict_pos, "*", "*", "*", "*", "*"]
                elif isinstance(jmdict_pos, list):
                    new_pos = jmdict_pos + ["*"] * max(0, 6 - len(jmdict_pos))
                else:
                    new_pos = list(token_list[j - 1]["pos"])
                
                lemma_str = match_word if isinstance(match_word, str) else combined_lemma
                reading = jmdict.get_reading(match_word, token_string)
                
                merged_tokens.append({
                    "surface": combined_surface,
                    "lemma": lemma_str,
                    "base_form": lemma_str,
                    "reading": reading,
                    "pos": new_pos,
                })
                i = j
                match_found = True
                break
        
        if not match_found:
            merged_tokens.append(token_list[i])
            i += 1

    return merged_tokens

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

    if pos1 in SKIP_POS1:
        return True

    if (pos1, pos2) in SKIP_POS1_POS2:
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
