#!/usr/bin/env python3
import argparse
from collections import OrderedDict
from pathlib import Path
import csv, sys
from tempfile import TemporaryDirectory
from os import path

from src.AddDefinitionsStep import AddDefinitionsStep
from src.AddReadingsStep import AddReadingsStep
from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline
from src.PipelineStep import DebugStep, NoOpStep, PipelineStep
from src.ProcessingStep import ProcessingStep
from src.TokenizeStep import TokenizeStep
from src.FilterFrequencyStep import FilterFrequencyStep
from src.WriteOutputStep import WriteOutputStep


MIN_FREQUENCY_DEFAULT = 3

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


print_debug = lambda *a: None


def enable_debug_logging():
    global print_debug
    print_debug = print


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


def print_step_progress(step, amount, total, additional_text=''):
    step_text = {
        step.TOKENIZING: 'Tokenizing',
        step.FILTERING: 'Filtering useful vocab',
        step.READINGS: 'Adding readings',
        step.DEFINITIONS: 'Adding definitions'
    }

    if step == step.TOKENIZING:
        additional_text = f'Total tokens: {amount}'

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

    initial_artifact = Artifact(input_path, is_path=True)

    pipeline = build_mining_pipeline(
        output_path=args.output,
        min_frequency=min_frequency,
        tags=args.tags,
    )

    pipeline.run(initial_artifact)

    print('pipeline completed')

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


def build_mining_pipeline(output_path, min_frequency, tags=None):
    steps = [
        TokenizeStep(),
        FilterFrequencyStep(min_frequency),
        AddReadingsStep(),
        AddDefinitionsStep(),
    ]

    # if tags:
    #     steps.append(AddTagsStep(tags))

    steps.append(WriteOutputStep(output_path))

    return Pipeline(
        steps = steps,
        on_progress = print_step_progress)


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
