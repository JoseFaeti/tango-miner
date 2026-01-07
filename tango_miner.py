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

from src.Pipeline import Pipeline
from src.PipelineStep import DebugStep, NoOpStep


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

# Order in which the word data is written to the final CSV file
CSV_FIELD_ORDER = [
    "word",
    "index",
    "frequency",
    "frequency_normalized",
    "reading",
    "definition",
    "tags",
]

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
        get_jamdict._instance = Jamdict(memory_mode = False)
    return get_jamdict._instance


def tokenize(input_path, output_path, word_data=None):
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
            
            if not lemma:
                continue

            token_index += 1

            if lemma in word_data:
                word_data[lemma][1] += 1
            else:
                word_data[lemma] = [token_index, 1, set()]
                
            if tag:
                word_data[lemma][2].add(tag)

    if not output_path:
        return word_data
    else:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)

            processed = 0
            total_words = len(word_data.items())
            total_tokens = token_index

            for word, (index, frequency, tags) in word_data.items():
                index_normalized = 1 - (index / total_tokens)
                frequency_normalized = frequency / total_tokens
                score = round(index_normalized * frequency_normalized * 10_000, 10)

                writer.writerow([word, index, frequency, score, " ".join(sorted(tags))])
                processed += 1
                print_step_progress(ProcessingStep.TOKENIZING, processed, total_words, f'Total tokens: {processed}')

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

            print_step_progress(ProcessingStep.FILTERING, processed, total_filtered, f'Vocab kept: {kept}/{total}')


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
            print_step_progress(ProcessingStep.DEFINITIONS, processed, total, f'{processed}/{total} ({cached} cached)')


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


def read_tokens_to_dict(file_path):
    """
    Reads a Tango Miner CSV/TMP file and returns a dictionary of tokens.
    Format returned:
    {
        'token1': {'frequency': 3, 'reading': 'よみ', 'definition': 'meaning', ...},
        'token2': {...},
        ...
    }
    """
    tokens = {}

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            token = row.get("token") or row.get("Token")

            if not token:
                continue

            # parse frequency as int
            try:
                frequency = int(row.get("frequency", 1))
            except ValueError:
                frequency = 1

            # store other fields as needed
            token_info = {
                "frequency": frequency,
                "reading": row.get("reading", ""),
                "definition": row.get("definition", ""),
            }

            # if token already exists, sum frequencies
            if token in tokens:
                tokens[token]["frequency"] += frequency
                # optionally merge readings/definitions if they differ
                if row.get("reading") and row.get("reading") not in tokens[token]["reading"]:
                    tokens[token]["reading"] += f";{row.get('reading')}"
                if row.get("definition") and row.get("definition") not in tokens[token]["definition"]:
                    tokens[token]["definition"] += f";{row.get('definition')}"
            else:
                tokens[token] = token_info

    return tokens


def write_final_file(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if not row or not row[0].strip():
                continue

            record = row_to_record(row)
            ordered_row = record_to_row(record, CSV_FIELD_ORDER)

            writer.writerow(ordered_row)


def row_to_record(row):
    return {
        "word": row[0] if len(row) > 0 else "",
        "index": row[1] if len(row) > 1 else "",
        "frequency": row[2] if len(row) > 2 else "",
        "frequency_normalized": row[3] if len(row) > 3 else "",
        "tags": row[4] if len(row) > 4 else "",
        "reading": row[5] if len(row) > 5 else "",
        "definition": row[6] if len(row) > 6 else "",
    }


def record_to_row(record, field_order):
    return [record.get(field, "") for field in field_order]


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
    parser.add_argument("--output", "-o", required=False, help="Path to output CSV file")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--minFrequency", "-f", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--debug", "-d", action='store_true')

    # Parse arguments
    args = parser.parse_args()
    input_path = args.input

    tags = args.tags
    min_frequency = args.minFrequency or MIN_FREQUENCY_DEFAULT

    debug = args.debug

    if debug:
        enable_debug_logging()

    print(f'Min frequency: {min_frequency}')
    print_debug('debug = true')

    pipeline = Pipeline([
        DebugStep("step-1"),
        NoOpStep(),
        DebugStep("step-2"),
    ])

    result = pipeline.run("initial input")

    return


    with TemporaryDirectory() as tmpdir:
        input_path_obj = Path(input_path)

        if input_path_obj.is_file():
            print_debug(f'Created temp dir: {tmpdir}')
            output_path = args.output or args.input + '.csv'
            mine_file(input_path, output_path, tmpdir, min_frequency, tags)
        elif input_path_obj.is_dir():
            print(f'Mining all relevant files from directory {input_path}...')

            output_path = Path(args.output)

            single_file_mode = False

            # create output path and intermetiade directories if necessary
            if output_path.exists():
                if output_path.is_dir():
                    final_path = output_path
                else:
                    final_path = output_path
                    single_file_mode = True
            else:
                if output_path.suffix:
                    final_path = output_path
                    # create missing directories if necessary
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    single_file_mode = True
                else:
                    final_path = output_path
                    output_path.mkdir(parents=True, exist_ok=True)

            print_debug(f'output_path: {output_path}')
            print_debug(f'output_path exists = {output_path.exists()}')
            print(f'output path: {final_path}')
            print_debug(f'single file mode = {single_file_mode}')
            
            combined_tokens = OrderedDict()
            tokens_file_path = Path(tmpdir) / 'tokens.tmp'

            # process directory contents
            for file in input_path_obj.iterdir():
                if file.is_file() and file.suffix.lower() in {".txt", ".csv", ".pdf", ".xml", ".html", ".srt"}:
                    if single_file_mode:
                        print(f"Tokenizing {file}...")
                        combined_tokens = tokenize(file, None, combined_tokens)
                    else:
                        mine_file(file, output_path / (file.name + '.csv'), tmpdir, min_frequency, tags)

            if single_file_mode:
                tokenize(None, tokens_file_path, combined_tokens)
                mine_file(tokens_file_path, final_path, tmpdir, min_frequency, skip_tokenize=True)


def mine_file(input_path, output_path, tmpdir, min_frequency=MIN_FREQUENCY_DEFAULT, tags=False, skip_tokenize=False):
    print(f'Mining vocabulary from {input_path}...')
    print_debug(f'mining from {input_path} to {output_path}')
    input_file = input_path

    if skip_tokenize:
        output_file = input_file
    else:
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
