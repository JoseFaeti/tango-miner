from collections import Counter
from fugashi import Tagger
from pathlib import Path
import re
import unidic
import csv

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .TokenCache import TokenCache
from .WordStats import WordStats, Sentence

# Regex for detecting Katakana-only and small kana endings
RE_ALL_KATAKANA = re.compile(r"^[ァ-ンー]+$")
RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")

# POS categories to skip — if any level of POS matches one of these, skip it
SKIP_POS = {
    "助詞",       # particle
    # "助動詞",     # auxiliary verb
    "記号",       # symbol/punctuation
    "感動詞",     # interjection
    # "接続詞",     # conjunction
    # "連体詞",     # prenominal adjective
    "フィラー",      # filler like "えーと"
    "その他",      # other
    "名詞-固有名詞", # proper noun
    "代名詞",     # pronoun
    "接頭辞",     # prefix
    "接尾辞",     # suffix
}
SKIP_POS_PREFIXES = tuple(SKIP_POS)

TOKENIZER_FINGERPRINT = "unidic-2.1.2+postproc-v1.2026/01/16"
SENT_BOUNDARY = "🐍"  # any char that will never appear naturally

MAX_SENTENCES = 3
MIN_SENTENCE_LENGTH = 7
MAX_SENTENCE_LENGTH = 30

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

        tagger = Tagger(f'-d "{Path(unidic.DICDIR)}"')
        tokens = [unidic_node_to_dict(n) for n in tagger(text)]

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
    sentence_endings = {"。", "！", "？", "・", SENT}

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
                current_sentence_length = 0
                continue

            if char != SENT:
                if len(current_sentence_tokens) > 0:
                    current_sentence_tokens.append(char)
                    current_sentence_length += 1
                else:
                    # First character of each sentence should not be punctuation or whitespace
                    if not (char.isspace() or char in NOT_ALLOWED_AT_SENTENCE_START):
                        current_sentence_tokens.append(char)
                        current_sentence_length += 1

            if char not in sentence_endings:
                continue

            sentence = "".join(current_sentence_tokens)
            sentence_length = current_sentence_length

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
            current_sentence_length = 0

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
            )
            word_data[lemma] = ws

        if tag:
            ws.tags.add(tag)

    cache.flush_mtime_index()

    return word_data


def sentence_exists(ws, sentence_text: str, tag: str) -> bool:
    return any(s.text == sentence_text and s.tag == tag for s in ws.sentences)


def unidic_node_to_dict(node) -> dict:
    f = node.feature  # UniDic feature object

    # Pick the lemma we will use everywhere
    lemma = (
        getattr(f, "orthBase", None)
        or getattr(f, "lemma", None)
        or node.surface
    )

    # Pick the matching reading for THAT lemma
    reading = (
        getattr(f, "kanaBase", None)   # reading of the base form
        or getattr(f, "kana", None)    # surface reading fallback
    )

    return {
        "surface": node.surface,
        "lemma": lemma,
        "base_form": lemma,   # keep compatibility with existing code
        "reading": kata_to_hira(reading),
        "pos": f"{f.pos1}-{f.pos2}",
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
    pos_list = token["pos"]

    # Local bindings (faster lookups)
    re_small_kana_end = RE_SMALL_KANA_END

    # Skip words ending with small tsu or elongation mark
    last = lemma[-1]
    if last in ("っ", "ッ", "ー"):
        return True

    # POS filtering (flattened)
    if token["pos"].startswith(SKIP_POS_PREFIXES):
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
        or c in "「（(【『…‥〜〜“<"
        # Spaces
        # or c in " \u3000"
        or c.isspace()
        or c in NOT_ALLOWED_AT_SENTENCE_START
    )

NOT_ALLOWED_AT_SENTENCE_START = "。、！？ー・・」）)』”>#%&+=*/:;@￥$^―_〜|\\-】'\""