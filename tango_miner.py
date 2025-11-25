#!/usr/bin/env python3
import argparse
from collections import OrderedDict
from fugashi import Tagger
from pathlib import Path
import unidic
import csv, sys
import re
import requests
import time
from collections import Counter
from enum import IntEnum
from jamdict import Jamdict
from tempfile import TemporaryDirectory
from os import path
import appdirs
import shelve


MIN_FREQUENCY_DEFAULT = 3

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

# Regex for detecting Katakana-only and small kana endings
RE_ALL_KATAKANA = re.compile(r"^[ァ-ンー]+$")
RE_SMALL_KANA_END = re.compile(r"[っゃゅょァィゥェォッャュョー]+$")

# Common priority tags — higher = more common
PRI_WEIGHTS = {
    "ichi1": 10000,
    "news1": 800,
    "spec1": 15000,
    "gai1": 10,
    "ichi2": 5000,
    "news2": 50,
    "spec2": 8000,
    "gai2": 9,
    # "nfXX" tags handled dynamically
}

class Column(IntEnum):
    WORD = 0
    INDEX = 1
    FREQUENCY = 2
    NORMALIZED_FREQUENCY = 3
    KANA = 4
    DEFINITION = 5

class ProcessingStep(IntEnum):
    TOKENIZING = 0
    FILTERING = 1
    READINGS = 2
    DEFINITIONS = 3

print_debug = lambda *a: None

tagger = Tagger()
cache = None


def open_cache():
    global cache

    print_debug('creating cache file...')
    # Cross-platform cache directory
    cache_dir = Path(appdirs.user_cache_dir("tango_miner"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "definitions.db"
    cache = shelve.open(str(cache_file), writeback=False)
    print_debug(f'cache file created: {cache} at {cache_file}')

def close_cache():
    global cache
    if cache is not None:
        print_debug('closing cache file...')
        cache.close()
        cache = None

def cache_definition(word, definition):
    global cache
    if cache.get(word) != definition:
        cache[word] = definition

def get_cached_definition(word):    
    global cache
    return cache.get(word)


def enable_debug_logging():
    global print_debug
    print_debug = print


def get_jamdict():
    if not hasattr(get_jamdict, "_instance"):
        get_jamdict._instance = Jamdict(memory_mode = True)
    return get_jamdict._instance


def tokenize(input_path, output_path):
    tagger = Tagger(f"-d \"{Path(unidic.DICDIR)}\"")

    with open(input_path, encoding='utf-8') as f:
        text = f.read()

    word_data = OrderedDict()
    token_index = 0
    
    for token in tagger(text):
        surface = token.surface.strip()
        
        if not surface:
            continue

        token_index += 1

        if surface in word_data:
            word_data[surface][1] += 1
        else:
            word_data[surface] = [token_index, 1]

    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)

        processed = 0
        total = len(word_data.items())

        for word, (index, frequency) in word_data.items():
            writer.writerow([word, index, frequency, frequency / total])
            processed += 1
            print_step_progress(ProcessingStep.TOKENIZING, processed, total, f'Total tokens: {processed}')

    # print(f'total tokens: {token_index}')


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


def filter_useful_words(input_csv: str, output_csv: str, min_frequency = MIN_FREQUENCY_DEFAULT):
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

            print_step_progress(ProcessingStep.FILTERING, processed, total_filtered, f'Filtered vocab: {kept}/{total}')


def add_readings(input_file, output_file):

    def kata_to_hira(text: str) -> str:
        """Convert katakana to hiragana."""
        result = []
        for ch in text:
            code = ord(ch)
            if 0x30A1 <= code <= 0x30F3:  # Katakana range
                ch = chr(code - 0x60)
            result.append(ch)
        return "".join(result)

    tagger = Tagger()

    total = get_total_lines(input_file)
    processed = 0

    with open(input_file, "r", encoding="utf-8") as fin, \
         open(output_file, "w", encoding="utf-8", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        for row in reader:
            if not row:
                continue
            word = row[0].strip()
            parsed = list(tagger(word))
            readings = []
            for m in parsed:
                reading = getattr(m.feature, "reading", "") or getattr(m.feature, "kana", "") or m.surface
                readings.append(reading)
            kana = kata_to_hira("".join(readings))
            writer.writerow(row + [kana])

            processed += 1
            print_step_progress(ProcessingStep.READINGS, processed, total, f'{processed}/{total}')


def best_entries(entries, search_word, tie_break="all"):
    """
    Selects the most common Jamdict entries based on priority tags
    from kanji and reading elements.
    
    tie_break='all' -> return all top-scoring entries
    tie_break='defs' -> return the one with the most definitions among the top ones
    """
    print_debug(f'best_entries({entries}, {search_word}, tie_break={tie_break})')

    if not entries:
        return []

    kana_only = re.fullmatch(r"[ぁ-んァ-ンー]+", search_word) is not None
    print_debug(f'kana only = {kana_only}')

    def score_forms(forms):
        score = 0

        for r in forms:
            print_debug(f'check form: {r} -> {r.pri}')

            tags = r.pri
            tag_found = False

            # if 'ichi1' in tags:
            #     score += PRI_WEIGHTS['ichi1']
            #     print(f'found tag ichi1 ({PRI_WEIGHTS["ichi1"]}) -> score = {score}')
            #     tag_found = True
            # else:
            for tag in tags:
              if tag.startswith('nf'):
                  score += (50 - int(tag[2:])) * 50
                  print_debug(f'found tag {tag[0:2]}[{tag[2:]}] ({(50 - int(tag[2:])) * 50}) -> score = {score}')
                  tag_found = True
              elif tag in PRI_WEIGHTS:
                  score += PRI_WEIGHTS[tag]
                  print_debug(f'found tag {tag} ({PRI_WEIGHTS[tag]}) -> score = {score}')
              else:
                  score += 1

            # no 'ichi1' or 'nfXX' tags found
            # if not tag_found:
            #   for tag in tags:
            #       if tag in PRI_WEIGHTS:
            #           score += PRI_WEIGHTS[tag]
            #           print(f'found tag {tag} ({PRI_WEIGHTS[tag]}) -> score = {score}')
            #       else:
            #           score += 1

        return score


    def score_entry(e):
        print_debug(f'score entry {e}')
        print_debug('score = 0')

        # Check kanji elements
        kanji_score = score_forms(e.kanji_forms) if not kana_only else 0
        kana_score = score_forms(e.kana_forms)

        print_debug(f'kanji score = {kanji_score}')
        print_debug(f'kana score = {kana_score}')

        return kanji_score + kana_score

    # Compute scores
    scored = [(e, score_entry(e)) for e in entries]
    print_debug("scored:")
    print_debug('\n'.join(f'[{score}] {term}' for term, score in scored))
    max_score = max(score for _, score in scored)

    print_debug(f'max score: {max_score}')

    if max_score == 0: return []

    top_entries = [e for e, score in scored if score == max_score]

    if tie_break == "all":
        return top_entries
    elif tie_break == "defs":
        # Among the tied ones, pick the entry with the most senses
        return [max(top_entries, key=lambda e: len(e.senses))]
    else:
        raise ValueError("tie_break must be 'all' or 'defs'")


def get_most_common_definition(word: str) -> str:
    result = get_jamdict().lookup(word)

    if not result.entries:
        return ""

    # Pick the entry with the highest score
    print_debug(result.entries)
    best_entries_result = best_entries(result.entries, word, tie_break="defs")

    if len(best_entries_result) == 0: return

    best_entry = best_entries_result[0] #max(result.entries, key=entry_rank)

    # Get the top 2 English glosses
    english_defs = []

    for i, s in enumerate(best_entry.senses):
        glosses = []

        if hasattr(s, "gloss"):
            glosses += (g.text for g in s.gloss)
        
        index = f'{i+1}. ' if len(best_entry.senses) > 1 else ''
        
        english_defs.append(f'{index}{"; ".join(glosses)}')

    final_definition = "<br>".join(english_defs)
    print_debug(f'final definition:\n{final_definition}\n')

    return final_definition


def add_and_filter_for_definitions(input_file, output_file):
    total = get_total_lines(input_file)
    processed = 0
    cached = 0

    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for i, row in enumerate(reader, start=1):
            if not row or not row[0].strip():
                continue

            word = row[0].strip()

            definition = get_cached_definition(word)

            if not definition:
                definition = get_most_common_definition(word)
                cache_definition(word, definition)
            else:
                cached += 1
                print_debug(f'found cached definition for {word}')

            if definition:
                row.append(definition)
                writer.writerow(row)

            # Show progress
            processed += 1
            print_step_progress(ProcessingStep.DEFINITIONS, processed, total, f'Vocab kept: {processed}/{total} ({cached} cached)')


def add_tags(input_file, output_file, tags):
    if not tags:
        return

    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for i, row in enumerate(reader):
            if not row or not row[0].strip():
                continue

            row.append(tags)
            writer.writerow(row)


def write_final_file(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for i, row in enumerate(reader):
            if not row or not row[0].strip():
                continue

            writer.writerow(row)


def get_total_lines(input_file):
    total = 0

    with open(input_file, "r", encoding="utf-8") as f:
        total = sum(1 for _ in f)

    return total


def print_step_progress(step, amount, total, additional_text=''):
    step_text = {
        step.TOKENIZING: 'Tokenizing',
        step.FILTERING: 'Filtering useful vocab',
        step.READINGS: 'Adding readings',
        step.DEFINITIONS: 'Adding definitions'
    }

    if amount == total:
        print(f"\r{step_text[step]}... done. {additional_text}", flush=True)
        return

    print(f"\r{step_text[step]}... {amount / total :.0%} {additional_text}", end="", flush=True)


def process_script():
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.")

    parser.add_argument("--input", "-i", required=True, help="Path to input text file")
    parser.add_argument("--output", "-o", required=True, help="Path to output CSV file")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--minFrequency", "-f", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--debug", "-d", action='store_true')

    # Parse arguments
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output

    tags = args.tags
    min_frequency = args.minFrequency or MIN_FREQUENCY_DEFAULT

    debug = args.debug

    if debug:
        enable_debug_logging()

    print(f'Mining vocabulary from {input_path}...')
    print(f'Min frequency: {min_frequency}')
    print_debug('debug = true')

    with TemporaryDirectory() as tmpdir:
        print_debug(f'Created temp dir: {tmpdir}')

        input_file = input_path
        output_file = path.join(tmpdir, '-1.tokenized.tmp')

        tokenize(input_file, output_file)

        input_file = output_file
        output_file = path.join(tmpdir, '-2.filtered.tmp')

        filter_useful_words(input_file, output_file, min_frequency)

        input_file = output_file
        output_file = path.join(tmpdir, '-3.readings.tmp')

        add_readings(input_file, output_file)

        input_file = output_file
        output_file = path.join(tmpdir, '-4.definitions.tmp')

        open_cache()
        add_and_filter_for_definitions(input_file, output_file)
        close_cache()

        if tags:
            input_file = output_file
            output_file = path.join(tmpdir, '-5.tags.tmp')

            print('adding tags...', end="", flush=True)
            add_tags(input_file, output_file, tags)
            print('done')

        input_file = output_file

        write_final_file(input_file, output_path)
        print(f'{output_path} generated successfully')


if __name__ == '__main__':
    process_script()
