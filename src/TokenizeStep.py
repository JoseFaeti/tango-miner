from collections import Counter, OrderedDict
from fugashi import Tagger
from pathlib import Path
import re
import unidic
import csv

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .TokenCache import TokenCache
from .WordStats import WordStats

# Regex for detecting Katakana-only and small kana endings
RE_ALL_KATAKANA = re.compile(r"^[ァ-ンー]+$")
RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")

# POS categories to skip — if any level of POS matches one of these, skip it
SKIP_POS = {
    "助詞",       # particle
    "助動詞",     # auxiliary verb
    "記号",       # symbol/punctuation
    "感動詞",     # interjection
    "接続詞",     # conjunction
    "連体詞",     # prenominal adjective
    "フィラー",      # filler like "えーと"
    "その他",      # other
    "名詞-固有名詞", # proper noun
    "代名詞",     # pronoun
    "接頭辞",     # prefix
    "接尾辞",     # suffix
}

TOKENIZER_FINGERPRINT = "unidic-2.1.2+postproc-v1.2026/01/16"

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        output_path = artifact.tmpdir / "-1.tokenized.tmp"
        word_data = tokenize(artifact.data, output_path, progress_handler=self.progress)
        return Artifact(word_data, is_path=True)


def tokenize(input_path, output_path, word_data=None, cache_dir=None, progress_handler=None):
    match = re.search(r"\[(.+?)\]", str(input_path))
    tag = match.group(1) if match else None

    token_index = 0

    if word_data is None:
        word_data = OrderedDict()
    else:
        token_index += 1

    input_path = Path(input_path)
    file_mtime = input_path.stat().st_mtime_ns  # cheap + precise

    cache = TokenCache(
        cache_dir=cache_dir,
        tokenizer_fingerprint=TOKENIZER_FINGERPRINT,
    )

    # --------------------------------------------------
    # 1. Fast path: mtime-based cache check
    # --------------------------------------------------
    cached = cache.get_by_mtime(input_path, file_mtime)

    if cached is not None:
        tokens = cached
    else:
        # --------------------------------------------------
        # 2. Slow path: read + hash + tokenize
        # --------------------------------------------------
        with open(input_path, encoding="utf-8") as f:
            text = f.read()

        tokens = cache.get(text)

        if tokens is None:
            tagger = Tagger(f'-d "{Path(unidic.DICDIR)}"')
            tokens = [unidic_node_to_dict(n) for n in tagger(text)]
            cache.put(text, tokens)

        # --------------------------------------------------
        # 3. Store mtime → token mapping
        # --------------------------------------------------
        cache.put_by_mtime(input_path, file_mtime, text, tokens)

    # --------------------------------------------------
    # Sentence extraction + stats (unchanged)
    # --------------------------------------------------
    current_sentence_tokens = []
    current_sentence_lemmas = []

    def is_japanese_char(c):
        return (
            "\u3040" <= c <= "\u309F"
            or "\u30A0" <= c <= "\u30FF"
            or "\uFF65" <= c <= "\uFF9F"
            or "\u4E00" <= c <= "\u9FFF"
            or "０" <= c <= "９"
            or c in "。、！？ー・「」（）"
        )

    sentence_endings = {"。", "！", "？", "「", "」"}

    for token in tokens:
        surface = token["surface"]

        if any(not is_japanese_char(c) for c in surface):
            current_sentence_tokens = []
            current_sentence_lemmas = []
            continue

        for char in surface:
            current_sentence_tokens.append(char)
            
            if char in sentence_endings:
                sentence = "".join(current_sentence_tokens).strip()
                
                if len(sentence) > 5:
                    sentence += f'<br>[{tag}]' if tag else ""
                    
                    for lemma in set(current_sentence_lemmas):
                        ws = word_data.get(lemma)
                        if ws and len(ws.sentences) < 3:
                            ws.sentences.append(sentence)
                
                current_sentence_tokens = []
                current_sentence_lemmas = []

        lemma = (token["base_form"] or token["lemma"] or "").strip()

        if not lemma or is_useless(token):
            continue

        current_sentence_lemmas.append(lemma)
        token_index += 1

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

    return word_data


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
