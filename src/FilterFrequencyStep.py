from collections import Counter
from fugashi import Tagger
from pathlib import Path
import csv
import re

from .Artifact import Artifact
from .Column import Column
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


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

tagger = Tagger()


class FilterFrequencyStep(PipelineStep):
    def __init__(self, min_frequency: int):
        self.min_frequency = min_frequency

    def process(self, artifact: Artifact) -> Artifact:
        output_path = Path("-2.filtered.tmp")
        filter_useful_words(artifact.data, output_path, self.min_frequency, self.progress)
        return Artifact(output_path, is_path=True)


def filter_useful_words(input_csv: str, output_csv: str, min_frequency: int, progress_handler=None):
    """Read words from input CSV, check POS for each, keep only real words."""
    with open(input_csv, "r", encoding="utf-8") as infile, \
         open(output_csv, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        kept = 0
        processed = 0

        word_list = list(reader)
        total = len(word_list)

        filtered = filter_for_min_occurrence(word_list, min_frequency)
        total_filtered = len(filtered)

        for row in filtered:
            if not row:
                continue

            word = row[Column.WORD].strip()

            if not word:
                continue

            processed += 1

            if not is_useless(word):
                writer.writerow(row)
                kept += 1

            progress_handler(ProcessingStep.FILTERING, processed, total_filtered)#, f'Vocab kept: {kept}/{total}')


def filter_for_min_occurrence(word_list, min_occurrence: int):
    """Filter out lines unless the word has an occurrence equal or greater to min_occurrence"""
    filtered = []

    for row in word_list:
        if not row or len(row) < 2:
            continue

        try:
            count = int(row[Column.FREQUENCY].strip())
        except ValueError:
            continue

        if count >= min_occurrence:
            filtered.append(row)

    return filtered


def is_useless(word: str) -> bool:
    """Return True if the word is noise or non-lexical."""
    if not word or len(word.strip()) <= 1:
        return True

    # Skip non-Japanese and Katakana-only
    if RE_ALL_KATAKANA.fullmatch(word):
        return True
    if not re.search(r"[ぁ-んァ-ン一-龯]", word):
        return True

    tokens = list(tagger(word))
    if not tokens:
        return True

    token = tokens[0]
    lemma = token.feature.lemma or token.surface
    pos_list = [token.feature.pos1, token.feature.pos2, token.feature.pos3, token.feature.pos4]

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
    if word.endswith(('っ', 'ッ', 'ー')):
        return True

    # Skip kana-only repeated characters (like はははははは)
    if all('ぁ' <= c <= 'ん' or c in "ーっッ" for c in word):
        counter = Counter(word)
        most_common_count = counter.most_common(1)[0][1]
        if most_common_count / len(word) > 0.6:
            return True

    return False
