#!/usr/bin/env python3

import argparse
from contextlib import redirect_stderr, redirect_stdout
import traceback
from dataclasses import replace
from pathlib import Path

# Allow selected agent commands to run as python -m tango_miner.<command>.
__path__ = [str(Path(__file__).resolve().parent / 'agent_commands')]
import sys
import appdirs
from tempfile import TemporaryDirectory
from os import path
import os
from urllib.request import Request, urlopen
from urllib.parse import urlsplit
from urllib.error import HTTPError

from src.Artifact import Artifact
from src.Column import Column
from src.Pipeline import Pipeline
from src.ConsoleProgress import ConsoleProgress
from src.RunConfig import resolve_config
from src.RunArchive import write_stage_record, create_run_archive, collect_dependencies, write_run_summary
from src.RunReport import write_run_report
from src.steps.CompareOutputStep.index import CompareOutputStep
from src.StageCache import StageCache

from src.steps.AddDefinitionsStep.index import AddDefinitionsStep
from src.steps.AddWordsToAnkiStep.index import AddWordsToAnkiStep
from src.steps.AttachSentencesStep.index import AttachSentencesStep
from src.steps.GatherInputFilesStep.index import GatherInputFilesStep
from src.steps.ExtractSentencesStep.index import ExtractSentencesStep
from src.steps.NormalizeSentences.index import NormalizeSentences
from src.steps.ProcessingStep.index import ProcessingStep
from src.steps.ReadFilesStep.index import ReadFilesStep
from src.steps.ScoreWordStep.index import ScoreWordStep
from src.steps.TokenizeStep.index import RawTokenizeStep
from src.steps.MergeTokensStep.index import MergeTokensStep
from src.steps.BuildWordStatsStep.index import BuildWordStatsStep
from src.steps.FilterFrequencyStep.index import FilterFrequencyStep
from src.steps.WriteOutputStep.index import WriteOutputStep
from src.steps.OccurrenceDiagnosticsStep.index import OccurrenceDiagnosticsStep
from src.Grammar import GrammarStep, GrammarPositionStep, AddGrammarCardsStep, ExcludeGrammarTokensStep

from src.steps.debug.DumpSentences.index import DumpSentences


MIN_FREQUENCY_DEFAULT = 4


print_debug = lambda *a: None
silent_archive_ready = False


def corpus_path_from_env(env_file):
    """Read the optional local corpus path without loading shell commands."""
    if not env_file.exists():
        return None
    entries = []
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == "TANGO_INPUT_PATH":
            entries.append(value.strip())
    if len(entries) != 1:
        raise ValueError(".env must contain exactly one TANGO_INPUT_PATH entry")
    value = entries[0]
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    if not value or not Path(value).is_absolute():
        raise ValueError("TANGO_INPUT_PATH must be an absolute directory path")
    return value


def enable_debug_logging():
    global print_debug
    print_debug = print


def process_script(argv=None):
    global silent_archive_ready
    silent_archive_ready = False
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="Process a Japanese text and extract word frequencies.", allow_abbrev=False)

    parser.add_argument("--input", "-i", required=False, help="Path to input text file")
    parser.add_argument("--output", "-o", required=False, help="CSV filename or output directory; runs are saved in its runs subdirectory")

    parser.add_argument("--tags", "-t", required=False, help="Tags to add to every word")
    parser.add_argument("--min-frequency", "--minFrequency", "-f", dest="min_frequency", type=int, required=False, help="Min amount of times a word needs to appear in the text to be included")
    parser.add_argument("--silent", action="store_true", help="Suppress run output; inspect pipeline.log and exit status")
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

    parser.add_argument("--workers", type=int, help="Parallel grammar and merge worker processes (default: config)")
    parser.add_argument("--occurrence-diagnostics", action="store_true",
                        help="Write detailed occurrence and candidate JSONL files (can be large)")

    parser.add_argument("--no-grammar", dest="grammar", action="store_false",
                        help="Disable source-supported grammar cards")

    args = parser.parse_args(argv)
    overrides = {}
    for section, key, value in (
        ("input", "path", args.input), ("input", "recursive", args.recursive),
        ("input", "encoding", args.encoding), ("output", "path", args.output),
        ("filter", "min_frequency", args.min_frequency), ("debug", "enabled", args.debug),
        ("parallel", "workers", args.workers),
    ):
        if value is not None:
            overrides.setdefault(section, {})[key] = value
    try:
        config_bytes = Path(args.config).read_bytes()
        with TemporaryDirectory() as config_tmp:
            initial_config = Path(config_tmp) / "config.toml"
            initial_config.write_bytes(config_bytes)
            config = resolve_config(initial_config, overrides)
        if config.input.path is None:
            default_input = corpus_path_from_env(Path(__file__).resolve().parent / ".env")
            if default_input is not None:
                config = replace(config, input=replace(config.input, path=default_input))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if args.show_config:
        print(config.to_json())
        return
    if args.silent and config.anki.enabled and not config.anki.output_only:
        parser.error("--silent requires Anki disabled or output-only")
    input_path = config.input.path
    if input_path is None:
        parser.error("an input path is required (--input, [input] path, or TANGO_INPUT_PATH in .env)")
    min_frequency = config.filter.min_frequency
    debug = config.debug.enabled
    recursive = config.input.recursive

    if debug and not args.silent:
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
        archived_config = final_path.parent / "config.toml"
        archived_config.write_bytes(config_bytes)
        config = resolve_config(archived_config, overrides)
        if config.input.path is None:
            config = replace(config, input=replace(config.input, path=input_path))

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
            ExcludeGrammarTokensStep(grammar_step) if grammar_step else None,
            MergeTokensStep(max_lookahead=config.merge.max_lookahead, dictionary_path=config.dictionary.jmdict_path,
                            diagnostics_path=final_path.parent / "rejected-merges.jsonl" if args.occurrence_diagnostics else None),
            OccurrenceDiagnosticsStep(final_path.parent / "occurrences.jsonl") if args.occurrence_diagnostics else None,
            GrammarPositionStep(grammar_step) if grammar_step else None,
            BuildWordStatsStep(merge_potential_forms=config.merge.merge_potential_forms,
                               dictionary_path=config.dictionary.jmdict_path),
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
        summary = {"status": "running", "dependencies": dependencies, "workers": config.parallel.workers,
                   "retokenize_requested": args.retokenize, "occurrence_diagnostics": args.occurrence_diagnostics, "grammar_enabled": args.grammar, "grammar_te_iru": args.grammar, "stages": [],
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
        silent_archive_ready = True
        log.write(f"Min frequency: {min_frequency}\nInput: {input_path_obj}\nOutput: {final_path}\n")
        silent_stream = open(os.devnull, "w", encoding="utf-8") if args.silent else None
        display = ConsoleProgress(silent_stream or sys.stdout, len(active_steps), verbose=args.verbose, log=log)

        def record_stage(index, step, record):
            write_stage_record(final_path.parent, index, step, record)
            if config.profiling.enabled and record["status"] != "running":
                write_performance(final_path.parent, pipeline.performance)
            display.stage(index, step, record)

        pipeline = Pipeline(steps=active_steps, on_progress=display.progress,
                            cache=stage_cache, on_stage_record=record_stage, profiling=config.profiling.enabled,
                            workers=config.parallel.workers)
        try:
            with display.capture_step_output():
                if args.silent:
                    with redirect_stderr(log):
                        pipeline.run(Artifact(input_path, tmpdir=tmpdir))
                else:
                    pipeline.run(Artifact(input_path, tmpdir=tmpdir))
            summary["status"] = "completed"
        except BaseException as exc:
            summary["status"] = "failed"
            summary["error"] = f"{type(exc).__name__}: {exc}"
            log.write(f"Pipeline failed: {summary['error']}\n")
            traceback.print_exc(file=log)
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
                try:
                    if args.silent:
                        with redirect_stdout(log), redirect_stderr(log):
                            notification_result = notify_run(summary, final_path.parent)
                    else:
                        notification_result = notify_run(summary, final_path.parent)
                    log.write(f"ntfy: {notification_result}\n")
                finally:
                    log.close()
                    if silent_stream is not None:
                        silent_stream.close()

    if not args.silent:
        print(f'All tasks completed in {pipeline.duration:.1f} seconds.')


def notify_run(summary, run_dir):
    """Send an optional notification without changing the pipeline result."""
    url = os.environ.get("NTFY_URL", "").strip()
    if not url:
        return "not configured"
    status = summary["status"]
    message = f"Tango Miner {status}\nRun: {run_dir.name}"
    if isinstance(summary.get("duration_seconds"), (int, float)):
        message += f"\nDuration: {summary['duration_seconds']:.1f} seconds"
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.path in ("", "/"):
            raise ValueError("NTFY_URL must be a full HTTP(S) topic URL")
        request = Request(url, data=message.encode("utf-8"), method="POST",
                          headers={"Title": f"Tango Miner {status}", "Content-Type": "text/plain; charset=utf-8"})
        with urlopen(request, timeout=10) as response:
            response.read()
        print("ntfy notification accepted by server.")
        return "accepted by server"
    except HTTPError as exc:
        print(f"Warning: ntfy notification failed (HTTP {exc.code}).", file=sys.stderr)
        return f"failed (HTTP {exc.code})"
    except ValueError:
        print("Warning: ntfy notification failed: NTFY_URL must be a full HTTP(S) topic URL.", file=sys.stderr)
        return "failed (invalid topic URL)"
    except Exception as exc:
        print(f"Warning: ntfy notification failed ({type(exc).__name__}).", file=sys.stderr)
        return f"failed ({type(exc).__name__})"


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
    try:
        process_script()
    except Exception:
        if "--silent" not in sys.argv[1:] or not silent_archive_ready:
            raise
        sys.exit(1)
