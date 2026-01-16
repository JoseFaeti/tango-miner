#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys, shutil
from tempfile import TemporaryDirectory
from os import path

from src.AddDefinitionsStep import AddDefinitionsStep
from src.AddReadingsStep import AddReadingsStep
from src.AddWordsToAnkiStep import AddWordsToAnkiStep
from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline
from src.PipelineStep import DebugStep, NoOpStep, PipelineStep
from src.ProcessingStep import ProcessingStep
from src.ScoreWordStep import ScoreWordStep
from src.TokenizeDirectoryStep import TokenizeDirectoryStep
from src.TokenizeStep import TokenizeStep
from src.FilterFrequencyStep import FilterFrequencyStep
from src.WriteOutputStep import WriteOutputStep


MIN_FREQUENCY_DEFAULT = 3


print_debug = lambda *a: None


def enable_debug_logging():
    global print_debug
    print_debug = print


_LAST_LEN = 0

def print_step_progress(step, amount, total, additional_text=""):
    if step is None:
        print(additional_text)
        return

    step_text = {
        step.TOKENIZING: "Tokenizing",
        step.FILTERING: "Filtering useful vocab",
        step.READINGS: "Adding readings",
        step.DEFINITIONS: "Adding definitions",
        step.SCORING: "Calculating scores",
        step.ANKI_EXPORT: "Sending words to Anki"
    }

    if amount >= total:
        _print_progress_line(f"{step_text[step]}... done. {additional_text}", newline=True)
    else:
        percent = f"{amount / total:.1%}"
        _print_progress_line(f"{step_text[step]}... {percent} {additional_text}", newline=False)


def _print_progress_line(text: str, newline: bool):
    global _LAST_LEN

    # Pad with spaces to fully overwrite previous output
    padded = text.ljust(_LAST_LEN)

    sys.stdout.write("\r" + padded)
    if newline:
        sys.stdout.write("\n")
        _LAST_LEN = 0
    else:
        _LAST_LEN = len(padded)

    sys.stdout.flush()


def process_script():
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.")

    parser.add_argument("--input", "-i", required=True, help="Path to input text file")
    parser.add_argument("--output", "-o", required=False, help="Path to output CSV file")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--minFrequency", "-f", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--debug", "-d", action='store_true')
    parser.add_argument("--recursive", "-r", required=False, action='store_true')

    # Parse arguments
    args = parser.parse_args()
    input_path = args.input

    tags = args.tags
    min_frequency = args.minFrequency or MIN_FREQUENCY_DEFAULT

    debug = args.debug
    recursive = args.recursive

    if debug:
        enable_debug_logging()

    print(f'Min frequency: {min_frequency}')
    print_debug('debug = true')
    print_debug(f'recursive mode = {recursive}')

    with TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        input_path_obj = Path(input_path)

        if input_path_obj.is_file():
            print_debug(f'Created temp dir: {tmpdir}')
            output_path = args.output or args.input + '.csv'
            mine_file(input_path, output_path, tmpdir, min_frequency, tags)
        elif input_path_obj.is_dir():
            print(f'Mining all relevant files from directory {input_path_obj.resolve()}...')

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

            print_debug(f'output_path: {output_path.resolve()}')
            print_debug(f'output_path exists = {output_path.exists()}')
            print(f'output path: {final_path.resolve()}')
            print_debug(f'single file mode = {single_file_mode}')

            steps = [
                TokenizeDirectoryStep(input_path_obj, include_subdirectories=recursive),
                FilterFrequencyStep(min_frequency),
                AddReadingsStep(),
                AddDefinitionsStep(),
                ScoreWordStep(),
                WriteOutputStep(output_path),
                AddWordsToAnkiStep()
            ]

            directory_pipeline = Pipeline(steps=steps, on_progress=print_step_progress)
            directory_pipeline.run(Artifact(input_path, tmpdir=tmpdir))

            # process directory contents
            # for file in input_path_obj.iterdir():
            #     if file.is_file() and file.suffix.lower() in {".txt", ".csv", ".pdf", ".xml", ".html", ".srt"}:
            #         if single_file_mode:
            #             print(f"Tokenizing {file}...")
            #             combined_tokens = tokenize(file, None, combined_tokens)
            #         else:
            #             mine_file(file, output_path / (file.name + '.csv'), tmpdir, min_frequency, tags)

            # if single_file_mode:
            #     tokenize(None, tokens_file_path, combined_tokens)
            #     mine_file(tokens_file_path, final_path, tmpdir, min_frequency, skip_tokenize=True)

    print('All tasks completed.')

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
    print(f'Mining vocabulary from {Path(input_path).resolve()}...')
    print_debug(f'mining from {input_path} to {output_path}')
    input_file = input_path
    
    initial_artifact = Artifact(input_path, tmpdir=tmpdir, is_path=True)

    pipeline = build_mining_pipeline(
        output_path=output_path,
        min_frequency=min_frequency,
        tags=tags,
    )

    pipeline.run(initial_artifact)

    print(f'{Path(output_path).resolve()} generated successfully')


if __name__ == '__main__':
    process_script()
