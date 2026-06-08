#!/usr/bin/env python3

import argparse
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from os import path

from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline

from src.steps.AddDefinitionsStep import AddDefinitionsStep
from src.steps.AddWordsToAnkiStep import AddWordsToAnkiStep
from src.steps.AttachSentencesStep import AttachSentencesStep
from src.steps.GatherInputFilesStep import GatherInputFilesStep
from src.steps.NormalizeSentenceBoundariesStep import NormalizeSentenceBoundariesStep
from src.steps.ProcessingStep import ProcessingStep
from src.steps.ReadFilesStep import ReadFilesStep
from src.steps.ScoreWordStep import ScoreWordStep
from src.steps.TokenizeStep import TokenizeStep
from src.steps.FilterFrequencyStep import FilterFrequencyStep
from src.steps.WriteOutputStep import WriteOutputStep


MIN_FREQUENCY_DEFAULT = 4


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
        step.SENTENCES: "Adding sentences",
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
    parser.add_argument("--user-dict", required=False)

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

        print(f'Mining all relevant files from directory {input_path_obj.resolve()}...')

        final_path = resolve_directory_output_path(input_path_obj, args.output)

        print(f'output path: {final_path.resolve()}')

        steps = [
            GatherInputFilesStep(input_path_obj, include_subdirectories=recursive),
            ReadFilesStep(),
            NormalizeSentenceBoundariesStep(),
            TokenizeStep(),
            FilterFrequencyStep(min_frequency),
            AddDefinitionsStep(),
            ScoreWordStep(),
            AttachSentencesStep(),
            WriteOutputStep(final_path),
            # AddWordsToAnkiStep()
        ]

        directory_pipeline = Pipeline(steps=steps, on_progress=print_step_progress)
        directory_pipeline.run(Artifact(input_path, tmpdir=tmpdir))

    print('All tasks completed.')


def resolve_directory_output_path(input_path: Path, output_arg: str | None) -> Path:
    input_path = Path(input_path)

    if output_arg is None:
        return input_path / f"{input_path.name}.csv"

    output_path = Path(output_arg)

    if output_path.exists() and output_path.is_dir():
        return output_path / f"{input_path.name}.csv"

    if output_path.suffix:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    output_path.mkdir(parents=True, exist_ok=True)
    return output_path / f"{input_path.name}.csv"


if __name__ == '__main__':
    process_script()
