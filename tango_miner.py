#!/usr/bin/env python3

import argparse
from pathlib import Path
import sys
import appdirs
from tempfile import TemporaryDirectory
from os import path

from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline
from src.ConsoleProgress import ConsoleProgress
from src.RunConfig import resolve_config
from src.RunArchive import write_stage_record, create_run_archive, collect_dependencies, write_run_summary
from src.RunReport import write_run_report
from src.steps.CompareOutputStep import CompareOutputStep
from src.StageCache import StageCache

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
from src.steps.OccurrenceDiagnosticsStep import OccurrenceDiagnosticsStep
from src.steps.GrammarShadowStep import GrammarShadowStep
from src.Grammar import GrammarStep, GrammarPositionStep, AddGrammarCardsStep, ExcludeGrammarTokensStep

from src.steps.debug.DumpSentences import DumpSentences


MIN_FREQUENCY_DEFAULT = 4


print_debug = lambda *a: None


def enable_debug_logging():
    global print_debug
    print_debug = print


def process_script(argv=None):
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.", allow_abbrev=False)

    parser.add_argument("--input", "-i", required=False, help="Path to input text file")
    parser.add_argument("--output", "-o", required=False, help="CSV filename or output directory; runs are saved in its runs subdirectory")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--min-frequency", "--minFrequency", "-f", dest="min_frequency", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--verbose", "-v", action="store_true", help="Keep progress for completed pipeline steps")
    parser.add_argument("--debug", "-d", action='store_true', default=None)
    parser.add_argument("--recursive", "-r", required=False, action='store_true', default=None)
    parser.add_argument("--user-dict", required=False)

    parser.add_argument("--config", default=Path(__file__).resolve().parent / "config.toml",
                        help="Load settings from a TOML file (default: config.toml)")
    parser.add_argument("--show-config", action="store_true", help="Print resolved configuration and exit")
    parser.add_argument("--encoding", help="Input text encoding")
    parser.add_argument("--retokenize", action="store_true",
                        help="Regenerate tokens for this run and refresh the tokenizer cache")

    parser.add_argument("--refresh-cache", action="append", choices=("merge", "all"), default=[],
                        help="Refresh the merge cache or all caches; may be repeated")

    parser.add_argument("--occurrence-diagnostics", action="store_true",
                        help="Write detailed occurrence and candidate JSONL files (can be large)")

    parser.add_argument("--grammar-shadow", action="store_true",
                        help="Write tentative grammar proposals without changing mining output")

    parser.add_argument("--no-grammar", dest="grammar", action="store_false",
                        help="Disable source-supported grammar cards")

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

    print_debug('debug = true')
    print_debug(f'recursive mode = {recursive}')

    with TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        input_path_obj = Path(input_path)

        print_debug(f"tmp dir: {tmpdir}")

        print_debug(f'Mining all relevant files from directory {input_path_obj.resolve()}...')

        output_path = resolve_directory_output_path(input_path_obj, config.output.path)
        final_path = create_run_archive(output_path, config)

        print_debug(f'Output path: {final_path.resolve()}')

        grammar_step = GrammarStep(max_examples=config.sentences.max_sentences, dictionary_path=config.dictionary.jmdict_path,
                                   prefer_expressions=True, max_lookahead=config.merge.max_lookahead,
                                   profile_rules=False, profiling=config.profiling.enabled) if args.grammar else None
        steps = [
            GatherInputFilesStep(input_path_obj, include_subdirectories=recursive, extensions=config.input.extensions,
                                 excluded_directory=final_path.parent.parent,
                                 manifest_path=final_path.parent / "input-manifest.json"),
            ReadFilesStep(encoding=config.input.encoding),
            ExtractSentencesStep(),
            DumpSentences("sentences.txt") if debug else None,
            NormalizeSentences(),
            DumpSentences("sentences-normalized.txt") if debug else None,
            RawTokenizeStep(force_retokenize=args.retokenize or "all" in args.refresh_cache, config=config.tokenizer),
            grammar_step,
            GrammarShadowStep(final_path.parent / "grammar-shadow.jsonl") if args.grammar_shadow else None,
            ExcludeGrammarTokensStep(grammar_step) if grammar_step else None,
            MergeTokensStep(max_lookahead=config.merge.max_lookahead, dictionary_path=config.dictionary.jmdict_path,
                            diagnostics_path=final_path.parent / "rejected-merges.jsonl" if args.occurrence_diagnostics else None),
            OccurrenceDiagnosticsStep(final_path.parent / "occurrences.jsonl") if args.occurrence_diagnostics else None,
            GrammarPositionStep(grammar_step) if grammar_step else None,
            BuildWordStatsStep(),
            FilterFrequencyStep(min_frequency, keep_percent=config.filter.keep_percent,
                                min_frequency_per_source=config.filter.min_frequency_per_source),
            AddDefinitionsStep(debug=debug, dictionary_path=config.dictionary.jmdict_path, diagnostics=args.occurrence_diagnostics),
            AddGrammarCardsStep(grammar_step) if grammar_step else None,
            ScoreWordStep(config.scoring),
            AttachSentencesStep(config.sentences),
            WriteOutputStep(final_path, diagnostics_path=(final_path.parent / "candidate-diagnostics.jsonl")
                            if args.occurrence_diagnostics else None),
            CompareOutputStep(final_path),
            AddWordsToAnkiStep(config.anki, plan_path=final_path.parent / "anki-plan.json") if config.anki.enabled else None,
        ]

        dependencies = collect_dependencies(config)
        summary = {"status": "running", "dependencies": dependencies,
                   "retokenize_requested": args.retokenize, "occurrence_diagnostics": args.occurrence_diagnostics, "grammar_shadow": args.grammar_shadow, "grammar_enabled": args.grammar, "grammar_te_iru": args.grammar, "stages": [],
                   "cache_refresh_requested": args.refresh_cache,
                   "anki": {"status": "not_started" if config.anki.enabled else "disabled"}}
        write_run_summary(final_path.parent, summary)
        from src.RunArchive import write_performance
        if config.profiling.enabled:
            write_performance(final_path.parent, {})
        stage_cache = StageCache(appdirs.user_cache_dir("tango_miner"), dependencies, args.refresh_cache)
        active_steps = [step for step in steps if step is not None]
        log_path = final_path.parent / "pipeline.log"
        log = log_path.open("w", encoding="utf-8", newline="\n")
        log.write(f"Min frequency: {min_frequency}\nInput: {input_path_obj}\nOutput: {final_path}\n")
        display = ConsoleProgress(sys.stdout, len(active_steps), verbose=args.verbose, log=log)

        def record_stage(index, step, record):
            write_stage_record(final_path.parent, index, step, record)
            if config.profiling.enabled and record["status"] != "running":
                write_performance(final_path.parent, pipeline.performance)
            display.stage(index, step, record)

        pipeline = Pipeline(steps=active_steps, on_progress=display.progress,
                            cache=stage_cache, on_stage_record=record_stage, profiling=config.profiling.enabled)
        try:
            with display.capture_step_output():
                pipeline.run(Artifact(input_path, tmpdir=tmpdir))
            summary["status"] = "completed"
        except BaseException as exc:
            summary["status"] = "failed"
            summary["error"] = f"{type(exc).__name__}: {exc}"
            log.write(f"Pipeline failed: {summary['error']}\n")
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
            try:
                write_run_summary(final_path.parent, summary)
                write_run_report(final_path.parent, summary)
            finally:
                if summary["status"] == "completed":
                    log.write(f"All tasks completed in {pipeline.duration:.1f} seconds.\n")
                log.close()

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
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        from src.ReportCLI import main
        sys.exit(main(sys.argv[2:]))
    process_script()
