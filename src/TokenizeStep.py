from pathlib import Path
import re
import unicodedata

from sudachipy import dictionary, tokenizer as sudachi_tokenizer

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .SegmentedSentence import SegmentedSentence
from .TokenCache import TokenCache
from .WordStats import WordStats

RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")

SKIP_POS1 = {
    "助詞",        # particles
    "記号",        # symbols (UniDic-style leftovers)
    "補助記号",    # Sudachi punctuation
    "感動詞",      # interjections
    "接頭辞",      # prefixes
    "接尾辞",      # suffixes
    "代名詞",      # pronouns
}

SKIP_POS1_POS2 = {
    ("名詞", "固有名詞"),    # proper nouns
    ("名詞", "代名詞"),      # pronouns (Sudachi also emits this)
    ("感動詞", "フィラー"),  # えーと, あの
}

TOKENIZER_FINGERPRINT = "sudachidict_full+user_dict.C+postproc-v1.2026/06/01.2"
MAX_SUDACHI_BYTES = 48000  # leave margin

SENT_BOUNDARY = "🐍"  # any char that will never appear naturally

MIN_SENTENCE_LENGTH = 7
MAX_SENTENCE_LENGTH = 50

tokenizer = None
TOKENIZER_MODE = sudachi_tokenizer.Tokenizer.SplitMode.C


class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        cache_dir = artifact.tmpdir / "token_cache" if artifact.tmpdir else None
        word_data, sentences = tokenize(
            artifact.data,
            cache_dir=cache_dir,
            progress_handler=self.progress,
        )
        return Artifact(word_data, sentences=sentences, is_path=True)


def tokenize(input_path, word_data=None, segmented_sentences=None, cache_dir=None, progress_handler=None):
    """
    Tokenize a single file.

    Returns (word_data, segmented_sentences) where:
      - word_data is a dict of lemma -> WordStats (frequencies, readings, etc.)
      - segmented_sentences is a list of SegmentedSentence objects found in this file
    """
    input_path = Path(input_path)

    match = re.search(r"\[(.+?)\]", str(input_path))
    tag = match.group(1) if match else None

    if word_data is None:
        word_data = {}

    if segmented_sentences is None:
        segmented_sentences = []

    file_mtime = input_path.stat().st_mtime_ns

    cache = TokenCache(
        cache_dir=cache_dir,
        tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
    )

    global tokenizer
    tokenizer = dictionary.Dictionary(config_path="resources/sudachi.json", dict="full").create()

    # ------------------------------
    # Cache fast path
    # ------------------------------
    hash_ = cache.get_hash_by_mtime(input_path, file_mtime)

    payload = cache.load_by_hash(hash_) if hash_ else None

    if payload:
        tokens = payload["tokens"]
    else:
        with open(input_path, encoding="utf-8") as f:
            text = f.read()

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = text.replace("\n", SENT_BOUNDARY)

        tokens = []

        for chunk in iter_sudachi_chunks(text):
            tokens.extend(sudachi_node_to_dict(m) for m in tokenizer.tokenize(chunk, TOKENIZER_MODE))

        cache.put(text, tokens)
        cache.put_by_mtime(input_path, file_mtime, text, tokens)

    total_tokens = len(tokens)

    # ------------------------------
    # Sentence and word accumulation
    # ------------------------------
    sentence_endings = {"。", "！", "？", SENT_BOUNDARY}

    current_sentence_chars = []
    current_sentence_lemmas = []    # ordered, may contain duplicates
    current_sentence_surfaces = {}  # lemma -> last surface form seen in this sentence

    token_index = 0

    for i, token in enumerate(tokens):
        if progress_handler and (i % 10000 == 0):
            progress_handler(ProcessingStep.TOKENIZING, i, total_tokens)

        token_index += 1
        surface = token["surface"]

        for char in surface:
            char = unicodedata.normalize("NFKC", char)

            # Build the sentence character buffer
            if char != SENT_BOUNDARY and is_japanese_char(char):
                if current_sentence_chars or (not char.isspace() and char not in NOT_ALLOWED_AT_SENTENCE_START):
                    current_sentence_chars.append(char)

            # On a sentence boundary, emit a SegmentedSentence if long enough
            if char in sentence_endings:
                sentence_text = "".join(current_sentence_chars)
                sentence_text = re.sub(r"\s+", "　", sentence_text)
                sentence_text = clean_sentence_text(sentence_text)

                if MIN_SENTENCE_LENGTH <= len(sentence_text) <= MAX_SENTENCE_LENGTH:
                    segmented_sentences.append(SegmentedSentence(
                        text=sentence_text,
                        tag=tag,
                        origin=input_path,
                        lemma_surfaces=dict(current_sentence_surfaces),
                    ))

                current_sentence_chars = []
                current_sentence_lemmas = []
                current_sentence_surfaces = {}

        # Accumulate word stats
        lemma = token["base_form"] or token["lemma"]
        reading = token["reading"]

        if not lemma or not reading or is_useless(token):
            continue

        current_sentence_lemmas.append(lemma)
        current_sentence_surfaces[lemma] = surface

        ws = word_data.get(lemma)
        if ws:
            ws.frequency += 1
        else:
            ws = WordStats(
                token_index,
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

    cache.flush_mtime_index()

    return word_data, segmented_sentences


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

_lemma_reading_cache = {}


def get_lemma_reading(lemma: str) -> str:
    if lemma in _lemma_reading_cache:
        return _lemma_reading_cache[lemma]
    m = tokenizer.tokenize(lemma, TOKENIZER_MODE)[0]
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


def clean_sentence_text(text: str) -> str:
    text = re.sub(r"\s+", "　", text).strip()

    if ">" not in text:
        return text

    prefix, body = text.split(">", 1)

    if body and contains_japanese_script(body) and not contains_japanese_script(prefix):
        return body.strip()

    return text


def contains_japanese_script(text: str) -> bool:
    return any(
        "\u3040" <= c <= "\u309F"
        or "\u30A0" <= c <= "\u30FF"
        or "\u4E00" <= c <= "\u9FFF"
        for c in text
    )


def iter_sudachi_chunks(text: str, max_bytes: int = MAX_SUDACHI_BYTES):
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")

    current = []
    current_bytes = 0

    for part in text.split(SENT_BOUNDARY):
        part_with_sep = part + SENT_BOUNDARY

        for piece in split_text_by_utf8_bytes(part_with_sep, max_bytes):
            piece_bytes = len(piece.encode("utf-8"))

            if current and current_bytes + piece_bytes > max_bytes:
                yield "".join(current)
                current = []
                current_bytes = 0

            current.append(piece)
            current_bytes += piece_bytes

    if current:
        yield "".join(current)


def split_text_by_utf8_bytes(text: str, max_bytes: int):
    current = []
    current_bytes = 0

    for char in text:
        char_bytes = len(char.encode("utf-8"))

        if current and current_bytes + char_bytes > max_bytes:
            yield "".join(current)
            current = []
            current_bytes = 0

        current.append(char)
        current_bytes += char_bytes

    if current:
        yield "".join(current)


def kata_to_hira(text: str) -> str:
    if not text:
        return None
    result = []
    for ch in text:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F3:  # Katakana range (ァ–ン)
            ch = chr(code - 0x60)
        result.append(ch)
    return "".join(result)


def is_useless(token: dict) -> bool:
    lemma = token["base_form"] or token["lemma"]

    # Skip words ending with small tsu or elongation mark
    if lemma[-1] in ("っ", "ッ", "ー"):
        return True

    pos1, pos2, *_ = token["pos"]

    if pos1 in SKIP_POS1:
        return True

    if (pos1, pos2) in SKIP_POS1_POS2:
        return True

    # Truncated stems like 言っ, しょっ
    if RE_SMALL_KANA_END.search(lemma):
        return True

    # Single kana noise
    if len(lemma) == 1:
        c = lemma[0]
        if "ぁ" <= c <= "ん" or "ァ" <= c <= "ン":
            return True

    has_japanese = False
    kana_only = True
    all_katakana = True
    char_freq = {}
    max_count = 0

    for c in lemma:
        if not ("ァ" <= c <= "ン" or c == "ー"):
            all_katakana = False

        if "一" <= c <= "龯":
            has_japanese = True
            kana_only = False
        elif "ぁ" <= c <= "ん" or c in "ーっッ":
            has_japanese = True
        else:
            kana_only = False

        if kana_only:
            count = char_freq.get(c, 0) + 1
            char_freq[c] = count
            if count > max_count:
                max_count = count

    if all_katakana or not has_japanese:
        return True

    if kana_only and max_count / len(lemma) > 0.6:
        return True

    return False


def is_japanese_char(c: str) -> bool:
    return (
        c == SENT_BOUNDARY
        or "\u3040" <= c <= "\u309F"   # Hiragana
        or "\u30A0" <= c <= "\u30FF"   # Katakana
        or "\uFF65" <= c <= "\uFF9F"   # Half-width Katakana
        or "\u4E00" <= c <= "\u9FFF"   # Kanji
        or c in "々〻ゝゞヽヾ"           # Iteration / repetition marks
        or "０" <= c <= "９"            # Full-width numbers
        or "0" <= c <= "9"             # ASCII numbers
        or "Ａ" <= c <= "Ｚ"            # Full-width Latin upper
        or "ａ" <= c <= "ｚ"            # Full-width Latin lower
        or "A" <= c <= "Z"             # ASCII Latin upper
        or "a" <= c <= "z"             # ASCII Latin lower
        or c in "（(【『…‥〜〜\u201C<"  # Common punctuation
        or c.isspace()
        or c in NOT_ALLOWED_AT_SENTENCE_START
    )


NOT_ALLOWED_AT_SENTENCE_START = "。、！？ー・・「」）)』\u201D>#%&+=*/:;@￥$^―_〜|\\-】'\""
