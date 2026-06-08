from pathlib import Path
import re
import unicodedata

from sudachipy import dictionary, tokenizer as sudachi_tokenizer

RE_TAG = re.compile(r"\[(.+?)\]")

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .SegmentedSentence import SegmentedSentence
from .TokenCache import TokenCache
from .WordStats import WordStats


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")

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

TOKENIZER_FINGERPRINT = "sudachidict_full+user_dict.C+postproc-v1.2026/06/01.2"
TOKENIZER_MODE = sudachi_tokenizer.Tokenizer.SplitMode.C

MAX_SUDACHI_BYTES = 48000

MIN_SENTENCE_LENGTH = 15
MAX_SENTENCE_LENGTH = 50

_tokenizer = None


def get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = dictionary.Dictionary(
            config_path="resources/sudachi.json",
            dict="full"
        ).create()
    return _tokenizer


# -------------------------------------------------------------------
# PIPELINE STEP
# -------------------------------------------------------------------

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        files: list[tuple[Path, list[str]]] = artifact.data
        cache_dir = artifact.tmpdir / "token_cache" if artifact.tmpdir else None

        self.combined_tokens = {}
        combined_sentences = []

        self.total_tokens = 0
        self.current_file = None
        self.sentence_offset = 0
        self.total_sentences = sum(len(sentences) for _, sentences in files)

        for path, sentences in files:
            self.current_file = path
            tag = RE_TAG.search(path.name)
            tag = tag.group(1) if tag else None

            self.combined_tokens, combined_sentences = tokenize(
                sentences,
                word_data=self.combined_tokens,
                segmented_sentences=combined_sentences,
                tag=tag,
                cache_dir=cache_dir,
                progress_handler=self._file_progress,
            )

            self.sentence_offset += len(sentences)
            self.total_tokens = len(self.combined_tokens)

        self.progress(
            ProcessingStep.TOKENIZING,
            1,
            1,
            f'{self.total_tokens} tokens from {len(files)} files',
        )

        return Artifact(self.combined_tokens, sentences=combined_sentences)


    def _file_progress(self, step, current, total, message=""):
        self.progress(
            step,
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
    cache_dir=None,
    progress_handler=None
):
    """
    NOTE:
    - input_path is now ALWAYS: list[str] (sentences)
    - file mode removed completely
    - no chunking, no fallback, no dual behavior
    """

    if not isinstance(input_path, list):
        raise ValueError("tokenize() now only accepts list[str] sentences")

    sentences_text = input_path

    cache = TokenCache(
        cache_dir=cache_dir,
        tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
    )

    tokenizer = get_tokenizer()

    word_data = word_data or {}
    segmented_out = list(segmented_sentences) if segmented_sentences else []

    current_sentence_chars = []
    current_sentence_surfaces = {}

    lemma_first_seen_index = {}
    token_index = 0

    sentence_endings = {"。", "！", "？"}

    # ------------------------------------------------------------
    # TOKENIZATION (SIMPLE + CORRECT)
    # ------------------------------------------------------------

    for i, sentence in enumerate(sentences_text):
        if progress_handler:
            progress_handler(
                ProcessingStep.TOKENIZING,
                i,
                len(sentences_text),
            )

        # tokenize FULL sentence (NO chunking)
        sudachi_tokens = tokenizer.tokenize(sentence, TOKENIZER_MODE)

        for m in sudachi_tokens:
            token = sudachi_node_to_dict(m)

            surface = unicodedata.normalize("NFKC", token["surface"])

            # --------------------------------------------------------
            # sentence reconstruction (unchanged logic)
            # --------------------------------------------------------

            for char in surface:
                if char in sentence_endings:
                    sentence_text = normalize_sentence("".join(current_sentence_chars))

                    for candidate in build_sentence_candidates(sentence_text):
                        if is_valid_sentence(candidate):
                            segmented_out.append(
                                SegmentedSentence(
                                    text=candidate,
                                    tag=tag,
                                    origin=None,
                                    lemma_surfaces=dict(current_sentence_surfaces),
                                )
                            )

                    current_sentence_chars.clear()
                    current_sentence_surfaces.clear()
                    continue

                if is_japanese_char(char):
                    current_sentence_chars.append(char)

            # --------------------------------------------------------
            # lemma processing (FIXED)
            # --------------------------------------------------------

            lemma = token["base_form"] or token["lemma"]
            reading = token["reading"]

            if not lemma or not reading or is_useless(token):
                continue

            # stable index (first occurrence only)
            if lemma not in lemma_first_seen_index:
                lemma_first_seen_index[lemma] = token_index
                token_index += 1

            current_sentence_surfaces[lemma] = surface

            # --------------------------------------------------------
            # frequency accumulation (CORRECT NOW)
            # --------------------------------------------------------

            ws = word_data.get(lemma)

            if ws:
                ws.frequency += 1
            else:
                ws = WordStats(
                    lemma_first_seen_index[lemma],
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

        # Flush at end of each input sentence
        if current_sentence_chars:
            sentence_text = normalize_sentence("".join(current_sentence_chars))
            for candidate in build_sentence_candidates(sentence_text):
                if is_valid_sentence(candidate):
                    segmented_out.append(
                        SegmentedSentence(
                            text=candidate,
                            tag=tag,
                            origin=None,
                            lemma_surfaces=dict(current_sentence_surfaces),
                        )
                    )
            current_sentence_chars.clear()
            current_sentence_surfaces.clear()

    return word_data, segmented_out


# -------------------------------------------------------------------
# CHUNKING (CLEAN REPLACEMENT)
# -------------------------------------------------------------------

def iter_sudachi_chunks(text: str, max_bytes: int = MAX_SUDACHI_BYTES):
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")

    current = []
    current_bytes = 0

    for piece in split_text_by_utf8_bytes(text, max_bytes):
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
        # Katakana range → Hiragana
        if 0x30A1 <= code <= 0x30F6:
            ch = chr(code - 0x60)
        result.append(ch)

    return "".join(result)


def is_japanese_char(c: str) -> bool:
    return (
        "\u3040" <= c <= "\u309F"   # Hiragana
        or "\u30A0" <= c <= "\u30FF"  # Katakana
        or "\u4E00" <= c <= "\u9FFF"   # Kanji
        or "\uFF65" <= c <= "\uFF9F"   # Half-width Katakana
        or c in "々〻ゝゞヽヾ"
        or c.isspace()
        or c.isdigit()
        or c.isalpha()
        or c in "（()【】「」『』…‥〜ー・"
    )


def is_useless(token: dict) -> bool:
    lemma = token["base_form"] or token["lemma"]

    # skip very short kana noise
    if not lemma:
        return True

    if lemma[-1] in ("っ", "ッ", "ー"):
        return True

    pos1, pos2, *_ = token["pos"]

    if pos1 in SKIP_POS1:
        return True

    if (pos1, pos2) in SKIP_POS1_POS2:
        return True

    # small kana endings (truncated forms)
    if RE_SMALL_KANA_END.search(lemma):
        return True

    # single kana noise
    if len(lemma) == 1:
        c = lemma[0]
        if "ぁ" <= c <= "ん" or "ァ" <= c <= "ン":
            return True

    # katakana-heavy noise detection
    all_katakana = True
    has_kanji = False

    for c in lemma:
        if "一" <= c <= "龯":
            has_kanji = True
        if not ("ァ" <= c <= "ン" or c == "ー"):
            all_katakana = False

    if all_katakana and not has_kanji:
        return True

    return False


import re

def normalize_sentence(text: str) -> str:
    if not text:
        return ""

    # normalize whitespace (including full-width spaces)
    text = re.sub(r"[ \t\u3000]+", "　", text)

    # collapse repeated whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # remove spaces around Japanese punctuation
    text = re.sub(r"\s*([、。！？「」『』])\s*", r"\1", text)

    # fix broken bracket cases
    if text.startswith("「") and "」" not in text:
        text = text[1:]
    if text.startswith("『") and "』" not in text:
        text = text[1:]
    if text.endswith("」") and "「" not in text:
        text = text[:-1]
    if text.endswith("』") and "『" not in text:
        text = text[:-1]

    # compress long punctuation runs
    text = re.sub(r"・{3,}", "・・・", text)
    text = re.sub(r"ー{3,}", "ーーー", text)

    return text


def build_sentence_candidates(text: str) -> list[str]:
    text = normalize_sentence(text)
    return [
        part
        for part in split_glued_dialogue_turns(text)
        if part and is_valid_sentence(part)
    ]


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


def split_glued_dialogue_turns(text: str) -> list[str]:
    parts = re.split(
        r"(?<=[。！？])(?=[ぁ-んァ-ン一-龯]{1,12}「)",
        text
    )
    return [p.strip("　 ") for p in parts if p.strip("　 ")]


def contains_japanese_script(text: str) -> bool:
    return any(
        "\u3040" <= c <= "\u309F"  # Hiragana
        or "\u30A0" <= c <= "\u30FF"  # Katakana
        or "\u4E00" <= c <= "\u9FFF"  # Kanji
        for c in text
    )


import re

def looks_like_guide_header(text: str) -> bool:
    guide_terms = (
        "行き方",
        "マップ",
        "攻略",
        "入手方法",
        "チャート",
        "戦闘開始時",
        "場合は",
    )

    if any(term in text for term in guide_terms):
        return True

    # pattern: Japanese text + control letter header-like fragments
    if re.search(r"^[ぁ-んァ-ン一-龯]+[　\s]+[A-ZＭＳLR]([　\s]|$)", text):
        return True

    return False