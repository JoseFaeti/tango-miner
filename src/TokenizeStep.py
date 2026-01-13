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

TOKENIZER_FINGERPRINT = "unidic-2.1.2+postproc-v1"

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        output_path = artifact.tmpdir / "-1.tokenized.tmp"
        word_data = tokenize(artifact.data, output_path, progress_handler=self.progress)
        return Artifact(word_data, is_path=True)


def tokenize(input_path, output_path, word_data=None, cache_dir=None, progress_handler=None):
    tag = None

    if input_path is not None:
        match = re.search(r"\[(.+?)\]", str(input_path))
        tag = match.group(1) if match else None

    token_index = 0

    if word_data is None:
        word_data = OrderedDict()
    else:
        # continue token index from the last entry
        token_index += 1

    with open(input_path, encoding='utf-8') as f:
        text = f.read()

    cache = TokenCache(cache_dir=cache_dir, tokenizer_fingerprint=TOKENIZER_FINGERPRINT)
    tokens = cache.get(text)

    if tokens is None:
        normalized_text = cache._normalize_text(text)
        tagger = Tagger(f"-d \"{Path(unidic.DICDIR)}\"")
        tokens = [unidic_node_to_dict(n) for n in tagger(text)]
        cache.put(text, tokens)

    for token in tokens:
        # print(token.feature)
        lemma = (token["base_form"] or token["lemma"] or "").strip()

        if not lemma or is_useless(token):
            continue

        token_index += 1

        if lemma in word_data:
            word_data[lemma].frequency += 1
        else:
            word_data[lemma] = WordStats(token_index, 1, 0.0, '', '', set(), set())
            
        if tag:
            word_data[lemma].tags.add(tag)

    return word_data


def unidic_node_to_dict(node) -> dict:
    f = node.feature  # UniDic feature object

    return {
        "surface": node.surface,
        "base_form": getattr(f, "orthBase", None),
        "lemma": getattr(f, "lemma", None),
        "reading": getattr(f, "reading", None),
        "pos": [f.pos1, f.pos2, f.pos3, f.pos4],
    }


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
