from collections import Counter, OrderedDict
from fugashi import Tagger
from pathlib import Path
import re
import unidic
import csv

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
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

class TokenizeStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        if artifact.is_path:
            output_path = artifact.tmpdir / "-1.tokenized.tmp"
            tokenize(artifact.data, output_path, progress_handler=self.progress)
            return Artifact(output_path, is_path=True)
        return artifact


def tokenize(input_path, output_path, word_data=None, progress_handler=None):
    tag = None

    if input_path is not None:
        match = re.search(r"\[(.+?)\]", str(input_path))
        tag = match.group(1) if match else None

    tagger = Tagger(f"-d \"{Path(unidic.DICDIR)}\"")

    if word_data is None:
        word_data = OrderedDict()
        token_index = 0
    else:
        # continue token index from the last entry
        token_index = max((v[0] for v in word_data.values()), default=0)

    if input_path is not None:
        with open(input_path, encoding='utf-8') as f:
            text = f.read()
        
        for token in tagger(text):
            # print(token.feature)
            lemma = token.feature.orthBase or token.feature.lemma
            
            if not lemma or is_useless(token):
                continue

            token_index += 1

            if lemma in word_data:
                word_data[lemma].frequency += 1
            else:
                word_data[lemma] = WordStats(token_index, 1, '', '', set(), set())
                
            if tag:
                word_data[lemma].tags.add(tag)

    # calculate normalized frequency
    total_words = len(word_data.items())
    total_tokens = token_index

    if not output_path:
        return word_data
    else:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)

            processed = 0

            for word, (index, frequency, tags) in word_data.items():
                index_normalized = 1 - (index / total_tokens)
                frequency_normalized = frequency / total_tokens
                score = round(index_normalized * frequency_normalized * 10_000, 10)

                writer.writerow([word, index, frequency, score, " ".join(sorted(tags))])
                processed += 1

                progress_handler(ProcessingStep.TOKENIZING, processed, total_words)#, f'Total tokens: {processed}')

    # print(f'total tokens: {token_index}')


def is_useless(token: str) -> bool:
    """Return True if the token is noise or non-lexical."""
    lemma = token.feature.orthBase or token.feature.lemma
    pos_list = [token.feature.pos1, token.feature.pos2, token.feature.pos3, token.feature.pos4]

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
