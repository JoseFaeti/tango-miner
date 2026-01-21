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
    "接続詞",     # conjunction
    # "連体詞",     # prenominal adjective
    "フィラー",      # filler like "えーと"
    "その他",      # other
    "名詞-固有名詞", # proper noun
    "代名詞",     # pronoun
    "接頭辞",     # prefix
    "接尾辞",     # suffix
}

TOKENIZER_FINGERPRINT = "unidic-2.1.2+postproc-v1.2026/01/16"
SENT_BOUNDARY = "🐍"  # any char that will never appear naturally

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        output_path = artifact.tmpdir / "-1.tokenized.tmp"
        word_data = tokenize(artifact.data, output_path, progress_handler=self.progress)
        return Artifact(word_data, is_path=True)


def tokenize(input_path, output_path, word_data=None, cache_dir=None, progress_handler=None):
    match = re.search(r"\[(.+?)\]", str(input_path))
    tag = match.group(1) if match else None

    if word_data is None:
        word_data = {}

    input_path = Path(input_path)
    file_mtime = input_path.stat().st_mtime_ns  # cheap + precise

    cache = TokenCache(
        cache_dir=cache_dir,
        tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
    )
    
    # --------------------------------------------------
    # 1. Fast path: mtime-based cache check
    # --------------------------------------------------
    mtime_ns = input_path.stat().st_mtime_ns
    hash = cache.get_hash_by_mtime(input_path, mtime_ns)

    if hash:
        payload = cache.load_by_hash(hash)
        tokens = payload["tokens"]
    else:
        # --------------------------------------------------
        # 2. Slow path: read + hash + tokenize
        # --------------------------------------------------
        with open(input_path, encoding="utf-8") as f:
            text = f.read()

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = text.replace("\n", SENT_BOUNDARY)

        tagger = Tagger(f'-d "{Path(unidic.DICDIR)}"')
        tokens = [unidic_node_to_dict(n) for n in tagger(text)]
        
        cache.put(text, tokens)
        cache.put_by_mtime(input_path, file_mtime, text, tokens)

    total_tokens = len(tokens)

    # --------------------------------------------------
    # Sentence extraction + stats
    # --------------------------------------------------
    sentence = ""
    current_sentence_tokens = []
    current_sentence_lemmas = []
    current_sentence_surfaces = {}

    sentence_endings = {"。", "！", "？", "・", SENT_BOUNDARY}

    token_index = 0

    for i, token in enumerate(tokens):
        if progress_handler and (i % 1000 == 0 or i == total_tokens):
            progress_handler(ProcessingStep.TOKENIZING, i, total_tokens)

        token_index += 1
        
        surface = token["surface"]

        for char in surface:
            if not is_japanese_char(char):
                current_sentence_tokens.clear()
                current_sentence_lemmas.clear()
                current_sentence_surfaces.clear()
                continue

            if char != SENT_BOUNDARY:
                current_sentence_tokens.append(char)
            
            if char not in sentence_endings:
                continue

            if len(current_sentence_tokens) < 4:
                continue

            sentence = "".join(current_sentence_tokens).strip()
            new_sentence_length = len(sentence)

            # print(f'sentence = {sentence}')

            lemmas_in_sentence = set(current_sentence_lemmas)
            
            for lemma in lemmas_in_sentence:
                ws = word_data.get(lemma)
                if not ws:
                    continue

                if sentence_exists(ws, sentence, tag):
                    continue

                sentences = ws.sentences
                surface_to_highlight = current_sentence_surfaces.get(lemma, "")

                if len(ws.sentences) < 3:
                    ws.sentences.append(
                        Sentence(sentence, tag, input_path, surface_to_highlight)
                    )

                elif new_sentence_length < 30:
                    existing_same_tag = [s for s in sentences if s.tag == tag]
                    
                    if existing_same_tag:
                        worst = min(existing_same_tag, key=lambda s: len(s.text))
                        if new_sentence_length > len(worst.text):
                            sentences.remove(worst)
                            sentences.append(
                                Sentence(sentence, tag, input_path, surface_to_highlight)
                            )
                    else:
                        # new tag, lemma full → replace globally worst if better
                        worst = min(sentences, key=lambda s: len(s.text))

                        if new_sentence_length > len(worst.text):
                            sentences.remove(worst)
                            sentences.append(
                                Sentence(sentence, tag, input_path, surface_to_highlight)
                            )

            current_sentence_tokens.clear()
            current_sentence_lemmas.clear()
            current_sentence_surfaces.clear()

        lemma = (token["base_form"] or token["lemma"] or "").strip()

        if not lemma or is_useless(token):
            continue

        current_sentence_lemmas.append(lemma)
        current_sentence_surfaces[lemma] = surface

        if lemma in word_data:
            ws = word_data[lemma]
            ws.frequency += 1
        else:
            ws = WordStats(
                token_index,
                1,
                0.0,
                kata_to_hira(token.get("reading") or ""),
                "",   # definition later
                set(),
                [],
                lemma,
                token['pos']
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
        "pos": [f.pos1, f.pos2, f.pos3, f.pos4],
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


def is_useless(token: str) -> bool:
    """Return True if the token is noise or non-lexical."""
    lemma = token["base_form"] or token["lemma"]
    pos_list = token["pos"]

    # Skip non-Japanese and Katakana-only
    if RE_ALL_KATAKANA.fullmatch(lemma):
        return True
    if not re.search(r"[ぁ-んァ-ン一-龯]", lemma):
        return True

    # Filter by POS
    for pos in pos_list:
        if not pos:
            continue
        for skip in SKIP_POS:
            if pos.startswith(skip):
                return True

    # Truncated stems like 言っ, しょっ, etc.
    if RE_SMALL_KANA_END.search(lemma):
        return True

    # Non-content single kana or noise
    if len(lemma) == 1 and re.match(r"[ぁ-んァ-ン]", lemma):
        return True

    # Skip words ending with small tsu or elongation mark
    if lemma.endswith(('っ', 'ッ', 'ー')):
        return True

    # Skip kana-only repeated characters (like はははははは)
    if all('ぁ' <= c <= 'ん' or c in "ーっッ" for c in lemma):
        counter = Counter(lemma)
        most_common_count = counter.most_common(1)[0][1]
        if most_common_count / len(lemma) > 0.6:
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
        or c in "。、！？ー・「」（）()【】『』…‥―〜〜“”\"'<>#%&+=*/:;@￥$^_〜|\\-"
        # Spaces
        # or c in " \u3000"
        or c.isspace()
    )

