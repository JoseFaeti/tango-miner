from collections import Counter
from pathlib import Path
import re
import csv

from sudachipy import dictionary, tokenizer as sudachi_tokenizer

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .TokenCache import TokenCache
from .WordStats import WordStats, Sentence

# Regex for detecting Katakana-only and small kana endings
RE_ALL_KATAKANA = re.compile(r"^[ァ-ンー]+$")
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
    ("名詞", "固有名詞"), # proper nouns
    ("名詞", "代名詞"),   # pronouns (Sudachi also emits this)
    ("感動詞", "フィラー"),  # えーと, あの
}

TOKENIZER_FINGERPRINT = "sudachidict_full+mode.C+postproc-v1.2026/02/26.9:29"
MAX_SUDACHI_BYTES = 48000  # leave margin

SENT_BOUNDARY = "🐍"  # any char that will never appear naturally

MAX_SENTENCES = 3
MIN_SENTENCE_LENGTH = 7
MAX_SENTENCE_LENGTH = 30


tokenizer = dictionary.Dictionary(dict_type="full").create()
TOKENIZER_MODE = sudachi_tokenizer.Tokenizer.SplitMode.C


class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        word_data = tokenize(artifact.data, progress_handler=self.progress)
        return Artifact(word_data, is_path=True)


def tokenize(input_path, word_data=None, cache_dir=None, progress_handler=None):
    input_path = Path(input_path)
    path_str = str(input_path)

    match = re.search(r"\[(.+?)\]", path_str)
    tag = match.group(1) if match else None

    if word_data is None:
        word_data = {}

    stat = input_path.stat()
    file_mtime = stat.st_mtime_ns

    cache = TokenCache(
        cache_dir=cache_dir,
        tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
    )

    # ------------------------------
    # Cache fast path
    # ------------------------------
    hash_ = cache.get_hash_by_mtime(input_path, file_mtime)

    if hash_:
        payload = cache.load_by_hash(hash_)
        tokens = payload["tokens"]
    else:
        with open(input_path, encoding="utf-8") as f:
            text = f.read()

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = text.replace("\n", SENT_BOUNDARY)

        tokens = []

        current = []
        current_bytes = 0

        for part in text.split(SENT_BOUNDARY):
            # Re-add the boundary we split on
            part_with_sep = part + SENT_BOUNDARY
            part_bytes = len(part_with_sep.encode("utf-8"))

            if current_bytes + part_bytes > MAX_SUDACHI_BYTES:
                chunk = "".join(current)
                tokens.extend(
                    sudachi_node_to_dict(m)
                    for m in tokenizer.tokenize(chunk, TOKENIZER_MODE)
                )
                current = []
                current_bytes = 0

            current.append(part_with_sep)
            current_bytes += part_bytes

        # Flush remainder
        if current:
            chunk = "".join(current)
            tokens.extend(
                sudachi_node_to_dict(m)
                for m in tokenizer.tokenize(chunk, TOKENIZER_MODE)
            )

        cache.put(text, tokens)
        cache.put_by_mtime(input_path, file_mtime, text, tokens)

    total_tokens = len(tokens)

    # ------------------------------
    # Local bindings (HOT PATH)
    # ------------------------------
    is_jp_char = is_japanese_char
    is_useless_local = is_useless
    sentence_exists_local = sentence_exists
    kata_to_hira_local = kata_to_hira
    WordStats_local = WordStats
    Sentence_local = Sentence
    word_data_get = word_data.get

    SENT = SENT_BOUNDARY
    sentence_endings = {"。", "！", "？", SENT}

    current_sentence_tokens = []
    current_sentence_lemmas = []
    current_sentence_surfaces = {}
    current_sentence_length = 0

    token_index = 0

    # ------------------------------
    # Main loop
    # ------------------------------
    for i, token in enumerate(tokens):
        if progress_handler and (i % 10000 == 0):
            progress_handler(ProcessingStep.TOKENIZING, i, total_tokens)

        token_index += 1
        surface = token["surface"]

        for char in surface:
            if not is_jp_char(char):
                current_sentence_tokens = []
                current_sentence_lemmas = []
                current_sentence_surfaces = {}
                continue

            if char != SENT:
                if len(current_sentence_tokens) > 0:
                    if char not in sentence_endings:
                        current_sentence_tokens.append(char)
                    elif char in "。！？ー）)』”%￥$〜】'\"":
                        current_sentence_tokens.append(char)
                else:
                    # First character of each sentence should not be punctuation or whitespace
                    if not char.isspace() and char not in NOT_ALLOWED_AT_SENTENCE_START:
                        current_sentence_tokens.append(char)

            if char not in sentence_endings:
                continue

            sentence = "".join(current_sentence_tokens)
            sentence = re.sub(r"\s{2,}", " ", sentence) # collapse whitespace
            sentence_length = len(current_sentence_tokens)

            if sentence_length < MIN_SENTENCE_LENGTH:
                continue

            lemmas_in_sentence = set(current_sentence_lemmas)

            for lemma in lemmas_in_sentence:
                ws = word_data_get(lemma)
                if not ws:
                    continue

                if sentence_exists_local(ws, sentence, tag):
                    continue

                sentences = ws.sentences
                surface_hl = current_sentence_surfaces.get(lemma, "")

                if len(sentences) < MAX_SENTENCES:
                    sentences.append(
                        Sentence_local(sentence, tag, input_path, surface_hl)
                    )
                elif sentence_length < MAX_SENTENCE_LENGTH:
                    worst = None
                    worst_text_length = 0

                    if tag:
                        for s in sentences:
                            if s.tag == tag and (worst is None or len(s.text) < worst_text_length):
                                worst = s
                                worst_text_length = len(worst.text)

                    if worst is None:
                        for s in sentences:
                            if worst is None or len(s.text) < worst_text_length:
                                worst = s
                                worst_text_length = len(worst.text)

                    if worst and sentence_length > worst_text_length:
                        sentences.remove(worst)
                        sentences.append(
                            Sentence_local(sentence, tag, input_path, surface_hl)
                        )

            current_sentence_tokens = []
            current_sentence_lemmas = []
            current_sentence_surfaces = {}

        lemma = token["base_form"] or token["lemma"]
        if not lemma:
            continue

        reading = token["reading"]
        if not reading:
            continue

        if is_useless_local(token):
            continue

        current_sentence_lemmas.append(lemma)
        current_sentence_surfaces[lemma] = surface

        ws = word_data_get(lemma)
        if ws:
            ws.frequency += 1
        else:
            ws = WordStats_local(
                token_index,
                1,
                0.0,
                kata_to_hira_local(reading),
                "",
                set(),
                [],
                lemma,
                token["pos"],
                invalid=False
            )
            word_data[lemma] = ws

        if tag:
            ws.tags.add(tag)

    cache.flush_mtime_index()

    return word_data


def sentence_exists(ws, sentence_text: str, tag: str) -> bool:
    return any(s.text == sentence_text and s.tag == tag for s in ws.sentences)


_lemma_reading_cache = {}

def get_lemma_reading(lemma: str) -> str:
    if lemma in _lemma_reading_cache:
        return _lemma_reading_cache[lemma]

    # tokenize lemma once
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


def kata_to_hira(text: str) -> str:
    if not text:
        return

    """Convert katakana to hiragana."""
    result = []
    for ch in text:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F3:  # Katakana range
            ch = chr(code - 0x60)
        result.append(ch)
    return "".join(result)


def is_useless(token: dict) -> bool:
    lemma = token["base_form"] or token["lemma"]

    # Local bindings (faster lookups)
    re_small_kana_end = RE_SMALL_KANA_END

    # Skip words ending with small tsu or elongation mark
    last = lemma[-1]
    if last in ("っ", "ッ", "ー"):
        return True

    # POS filtering
    pos1, pos2, *_ = token["pos"]

    if pos1 in SKIP_POS1:
        return True

    if (pos1, pos2) in SKIP_POS1_POS2:
        return True

    # Truncated stems like 言っ, しょっ
    if re_small_kana_end.search(lemma):
        return True

    # Single kana noise
    if len(lemma) == 1:
        c = lemma[0]
        if "ぁ" <= c <= "ん" or "ァ" <= c <= "ン":
            return True

    has_japanese = False
    kana_only = True
    all_katakana = True
    freq = {}
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
            count = freq.get(c, 0) + 1
            freq[c] = count
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
        # Hiragana
        or "\u3040" <= c <= "\u309F"
        # Katakana
        or "\u30A0" <= c <= "\u30FF"
        # Half-width Katakana
        or "\uFF65" <= c <= "\uFF9F"
        # Kanji
        or "\u4E00" <= c <= "\u9FFF"
        # Iteration / repetition marks (kanji & kana)
        or c in "々〻ゝゞヽヾ"
        # Full-width numbers
        or "０" <= c <= "９"
        # ASCII numbers
        or "0" <= c <= "9"
        # Full-width Latin letters
        or "Ａ" <= c <= "Ｚ"
        or "ａ" <= c <= "ｚ"
        # ASCII Latin letters
        or "A" <= c <= "Z"
        or "a" <= c <= "z"
        # Common punctuation & symbols
        or c in "（(【『…‥〜〜“<"
        # Spaces
        # or c in " \u3000"
        or c.isspace()
        or c in NOT_ALLOWED_AT_SENTENCE_START
    )

NOT_ALLOWED_AT_SENTENCE_START = "。、！？ー・・「」）)』”>#%&+=*/:;@￥$^―_〜|\\-】'\""