#!/usr/bin/env python3

import argparse
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from os import path

from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline
from src.RunConfig import resolve_config
from src.RunArchive import create_run_archive, collect_dependencies, write_run_summary

from src.steps.AddDefinitionsStep import AddDefinitionsStep
from src.steps.AddWordsToAnkiStep import AddWordsToAnkiStep
from src.steps.AttachSentencesStep import AttachSentencesStep
from src.steps.GatherInputFilesStep import GatherInputFilesStep
from src.steps.ExtractSentencesStep import ExtractSentencesStep
from src.steps.NormalizeSentences import NormalizeSentences
from src.steps.ProcessingStep import ProcessingStep
from src.steps.ReadFilesStep import ReadFilesStep
from src.steps.ScoreWordStep import ScoreWordStep
from src.steps.TokenizeStep import RawTokenizeStep
from src.steps.MergeTokensStep import MergeTokensStep
from src.steps.BuildWordStatsStep import BuildWordStatsStep
from src.steps.FilterFrequencyStep import FilterFrequencyStep
from src.steps.WriteOutputStep import WriteOutputStep

from src.steps.debug.DumpSentences import DumpSentences


MIN_FREQUENCY_DEFAULT = 4


print_debug = lambda *a: None


def enable_debug_logging():
    global print_debug
    print_debug = print


_LAST_LEN = 0

def print_step_progress(step, amount, total, duration=None, additional_text=""):
    if step is None:
        print(additional_text)
        return

    step_text = {
        step.TOKENIZING: "Tokenizing",
        step.TOKEN_MERGING: "Merging token compounds",
        step.WORD_COLLECTION: "Collecting tokens",
        step.FILTERING: "Filtering useful vocab",
        step.READINGS: "Adding readings",
        step.DEFINITIONS: "Adding definitions",
        step.SCORING: "Calculating scores",
        step.SENTENCES: "Adding sentences",
        step.ANKI_EXPORT: "Sending words to Anki",
        step.SENTENCE_EXTRACTION: "Extracting sentences",
        step.SENTENCE_NORMALIZATION: "Normalizing sentences",
        step.READ_FILES: "Reading files"
    }

    text = step_text[step] if step in step_text else "???"

    if amount >= total:
        if duration is not None:
            _print_progress_line(f"{text}... done in {duration:.1f} seconds. {additional_text}", newline=True)
        else:
            _print_progress_line(f"{text}... done. {additional_text}", newline=True)
    else:
        percent = f"{amount / total:.1%}"
        _print_progress_line(f"{text}... {percent} {additional_text}", newline=False)


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


def process_script(argv=None):
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.")

    parser.add_argument("--input", "-i", required=False, help="Path to input text file")
    parser.add_argument("--output", "-o", required=False, help="CSV filename or output directory; runs are saved in its runs subdirectory")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--min-frequency", "--minFrequency", "-f", dest="min_frequency", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--debug", "-d", action='store_true', default=None)
    parser.add_argument("--recursive", "-r", required=False, action='store_true', default=None)
    parser.add_argument("--user-dict", required=False)

    parser.add_argument("--config", default=Path(__file__).resolve().parent / "config.toml",
                        help="Load settings from a TOML file (default: config.toml)")
    parser.add_argument("--show-config", action="store_true", help="Print resolved configuration and exit")
    parser.add_argument("--encoding", help="Input text encoding")
    parser.add_argument("--retokenize", action="store_true",
                        help="Regenerate tokens for this run and refresh the tokenizer cache")

    args = parser.parse_args(argv)
    overrides = {}
    for section, key, value in (
        ("input", "path", args.input), ("input", "recursive", args.recursive),
        ("input", "encoding", args.encoding), ("output", "path", args.output),
        ("filter", "min_frequency", args.min_frequency), ("debug", "enabled", args.debug),
    ):
        if value is not None:
            overrides.setdefault(section, {})[key] = value
    try:
        config = resolve_config(args.config, overrides)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if args.show_config:
        print(config.to_json())
        return
    input_path = config.input.path
    if input_path is None:
        parser.error("an input path is required (--input or [input] path)")
    min_frequency = config.filter.min_frequency
    debug = config.debug.enabled
    recursive = config.input.recursive

    if debug:
        enable_debug_logging()

    print(f'Min frequency: {min_frequency}')
    print_debug('debug = true')
    print_debug(f'recursive mode = {recursive}')

    with TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        input_path_obj = Path(input_path)

        print_debug(f"tmp dir: {tmpdir}")

        print(f'Mining all relevant files from directory {input_path_obj.resolve()}...')

        output_path = resolve_directory_output_path(input_path_obj, config.output.path)
        final_path = create_run_archive(output_path, config)

        print(f'output path: {final_path.resolve()}')

        steps = [
            GatherInputFilesStep(input_path_obj, include_subdirectories=recursive, extensions=config.input.extensions,
                                 excluded_directory=final_path.parent.parent,
                                 manifest_path=final_path.parent / "input-manifest.json"),
            ReadFilesStep(encoding=config.input.encoding),
            ExtractSentencesStep(),
            DumpSentences("sentences.txt") if debug else None,
            NormalizeSentences(),
            DumpSentences("sentences-normalized.txt") if debug else None,
            RawTokenizeStep(force_retokenize=args.retokenize, config=config.tokenizer),
            MergeTokensStep(max_lookahead=config.merge.max_lookahead, dictionary_path=config.dictionary.jmdict_path),
            BuildWordStatsStep(),
            FilterFrequencyStep(min_frequency, keep_percent=config.filter.keep_percent,
                                min_frequency_per_source=config.filter.min_frequency_per_source),
            AddDefinitionsStep(debug=debug, dictionary_path=config.dictionary.jmdict_path),
            ScoreWordStep(config.scoring),
            AttachSentencesStep(config.sentences),
            WriteOutputStep(final_path),
            AddWordsToAnkiStep(config.anki) if config.anki.enabled else None,
        ]

        summary = {"status": "running", "dependencies": collect_dependencies(config),
                   "retokenize_requested": args.retokenize, "stages": [],
                   "anki": {"status": "not_started" if config.anki.enabled else "disabled"}}
        write_run_summary(final_path.parent, summary)
        pipeline = Pipeline(steps=[step for step in steps if step is not None], on_progress=print_step_progress)
        try:
            pipeline.run(Artifact(input_path, tmpdir=tmpdir))
            summary["status"] = "completed"
        except BaseException as exc:
            summary["status"] = "failed"
            summary["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            records = getattr(pipeline, "stage_records", [])
            summary["stages"] = records if isinstance(records, list) else []
            for record in summary["stages"]:
                if "anki" in record:
                    summary["anki"] = record["anki"]
            duration = getattr(pipeline, "duration", None)
            if isinstance(duration, (int, float)):
                summary["duration_seconds"] = duration
            write_run_summary(final_path.parent, summary)

    print(f'All tasks completed in {pipeline.duration:.1f} seconds.')


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
