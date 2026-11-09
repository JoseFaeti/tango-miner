# Development

[Contents](../usage-reference.md)


Example selection ranks sentences that meet the normal quality checks ahead of
short, long, or otherwise low-quality source lines, but retains those lines as
fallbacks. It prefers occurrences supporting the selected definition. If no
compatible source example is available, one conflicting occurrence is attached
and the candidate is marked for review (`no_definition_compatible_example` in
candidate diagnostics). Repeated identical lines do not create distinct examples;
frequency therefore need not equal the number of available examples.
Source-boundary repairs also distinguish the colloquial ズラかろう and selected
からさ contexts from analyzer misreadings of からい. Other ambiguous からさ
occurrences retain the analyzer's adjective analysis for review.
Dictionary expression merging also recognizes inflected ピンと来る, including
the dialectal negative ピンとこん, while retaining the source components.
For example, past-tense occurrences of できる can be assigned to that verb,
while the separate できた candidate can retain a supported prenominal or
interjection meaning. Ambiguous occurrences can still inflate its frequency.

## JMdict test baseline

Integration tests use the configured English-only legacy `JMdict_e` XML. The
tested snapshot was generated **2026-10-31**; the SHA-256 of the uncompressed
XML is `3214e9b77728d3d0e67a8bf5609d25b3bc8922e0103f1c5538fb74b078e81f41`.
This is a data dependency: different editions can change token merging and
definitions. It is not the `JMdict_e_NG` format.

Set `[dictionary].jmdict_path` in `config.toml` to the uncompressed XML, then
run `python scripts/check_jmdict.py` from the repository root. The check returns
nonzero for a missing file or hash mismatch. Obtain the matching snapshot from
a trusted retained copy if available. [EDRDG's distribution](https://www.edrdg.org/pub/Nihongo/00INDEX.html)
provides the current `JMdict_e.gz`, but its daily download may no longer match
the tested snapshot; decompress it before use and check the hash. If the exact
snapshot is unavailable, report that limitation and the actual hash when using
a different edition. Users may choose another edition; evaluate resulting
test failures and behavioral differences before updating this baseline.
Run archives record the actual dictionary hash.

## Performance optimization

After each optimization, analyze the remaining bottlenecks and propose the
most useful next step. Report measurement limitations, including differences in
inputs, configuration, dependencies, cache state and machine load where relevant.

To profile the merger on a bounded sample of an archived sentence CSV, run:

```sh
python scripts/profile_token_merging.py RUN_DIR/input.sentence.csv --reserve-grammar --diagnostics --repeat 3
```

The sample is limited by `--limit` and `--per-tag`. The report separates
merging time from checksum serialization and includes a checksum of complete
token output and rejection records. Omit `--reserve-grammar` or `--diagnostics`
to measure those configurations; `--max-lookahead` selects the merge limit.
Setup and grammar reservation are outside the reported merge time. Pass
`--profile FILE` to write cProfile data. Compare the same sample, options and
dictionary snapshot; archived examples are a selected sample, not a corpus run.

## Agent command modules

See the [agent command reference](../../agent_commands/README.md) for the
available modules and their shared options.

## Tests

Dictionary-dependent tests use `[dictionary].jmdict_path` from `config.toml` in
the current directory. If there is no configuration file, they use the default
`~/JMdict_e.xml`. Install the Sudachi full dictionary as described in the README.

Run the test suite from the repository root:

```sh
python scripts/run_tests.py
```

The runner emits failure details only and returns a nonzero exit status on failure.
Step tests live alongside their implementations in each step folder; core module and script tests live in `tests` folders alongside those areas; grammar rule tests live in each rule folder; cross-component tests and shared helpers remain in the
top-level `tests` folder. Pass a full unittest module name such as
`src.tests.test_run_report`, or discovery arguments, for focused runs. On failure,
the runner also reports a missing or mismatched JMdict baseline; it does not
require the baseline for ordinary use or otherwise passing tests.

Query one part of an archive with `python tango_miner.py report summary`,
`warnings`, `diff --previous`, `anki --changes-only`, or
`performance --regressions`. Add `--run RUN` to select an archive,
`--runs-dir DIR` to select an archive root, and `--json` for structured output.
The diff uses the recorded CSV comparison baseline; performance and warnings
use the preceding completed run or `--baseline RUN`, with `--threshold PERCENT`
(default 25). Timings are single-run signals, not sustained regressions; compare
input, configuration, dependencies, cache state and machine load. Anki counts
are plans and the status indicates whether they were applied. Missing records
are unavailable, not zero. These commands never mine or contact Anki.

For a single bounded check of the two latest completed archives, including
sentence coverage, compatibility, warnings, timings, and the Anki plan, run:

```sh
python -X utf8 scripts/compare_runs.py --overview --word できた
```

Repeat `--word` for candidate frequency and status comparisons. Omit it for a
general overview; pass two run directories to compare specific completed runs.
Sentence counts describe exported examples, not occurrence-level accuracy.

Compare the two most recent completed run archives without rerunning the
pipeline or contacting Anki:

```sh
python -X utf8 scripts/compare_runs.py --word しれる --word かもしれない
```

Use `--json-output PATH` to save the structured comparison of valid candidates for any two runs.
Pass two run directories to compare particular runs, or use `--runs-dir` to
select another archive root. The report shows candidate additions, removals,
status and frequency changes, input/config equality, and recorded Anki outcomes.
The dropped CSV combines invalid and unresolved candidates; a candidate
removed from output does not by itself identify a deleted Anki note.

For a short, targeted inspection of the latest completed archive, use:

```sh
python -X utf8 scripts/inspect_run.py --word する --word いる --examples 1
python -X utf8 scripts/inspect_run.py --status review --limit 10
python -X utf8 scripts/inspect_run.py --word ないし --rejected-merges --examples 2
python -X utf8 scripts/compare_runs.py --compact --word する --limit 5
```

`inspect_run.py` also accepts a run directory, `--contains`, and output limits
(`--limit`, `--examples`, `--width`). With `--word` and `--occurrences`, it
streams the run's `occurrences.jsonl` and prints a bounded analysis summary;
this requires a run made with occurrence diagnostics enabled. Occurrence totals
include tokens excluded before candidate collection. The dropped/review filter
also includes invalid candidates.

For other JSONL diagnostics, search a field or the whole record without loading
the file into memory or running the pipeline:

```sh
python -X utf8 scripts/query_jsonl.py runs/example-run/occurrences.jsonl --field sentence --contains ないし --show candidate --show sentence
```

Dotted fields traverse arrays; repeated `--show` selects fields. Output is
bounded by `--limit` and `--width`, but the whole file is scanned to count
matches. These records can include source text and paths.

Full corpus checks use the agreed input corpus in the private `.env` described
in [Running](running.md). The wrapper below accepts that path explicitly, resolves
the configuration, explicitly disables Anki, saves output to a log, waits for
completion, and reports the resulting archive. It can optionally compare a
completed baseline and selected candidates:

```sh
python -X utf8 scripts/check_corpus.py --input path/to/agreed-corpus --output-dir path/to/check-output --baseline runs/example-run --word candidate
```

Omit `--baseline` and `--word` when no comparison is needed; see `--help` for
other options. Check the resolved configuration and input before any full run.
The log and archive may contain source text and paths.

Replay a registered grammar rule against an existing historical diagnostic
archive without running the pipeline (new runs do not create these archives):

```sh
python -X utf8 scripts/replay_grammar.py runs/example-run/grammar-shadow.jsonl --rule koto_ga_aru_occasional --limit 10
```

The replay counts raw matches; it does not apply dictionary expression
precedence or example eligibility.

For detailed parsing investigation, add `--occurrence-diagnostics` to a run.
It writes `occurrences.jsonl`, `rejected-merges.jsonl`, and `candidate-diagnostics.jsonl` in the run
archive. These files can be very large and include source text and source
paths. Each occurrence records its normalized sentence, source, token position,
aligned character span (or null if alignment failed), candidate, raw component
evidence, collection eligibility, and the final token analysis rule. The
candidate file records final output status, frequency, reading and definition
for candidates surviving frequency filtering. Join on `candidate`; an eligible
occurrence missing from the candidate file did not survive that filter. Rejected merge records cover dictionary matches actually considered by the greedy merger, with the guard reason and source sentence; they do not include unmatched sequences or alternatives skipped after a successful merge. Candidate diagnostics include review reasons and the selected JMdict entry sequence and forms when available. Older archives may not contain review reasons. Derived potential definitions have no selected entry recorded. The analysis rule does not imply a confidence score. Existing
archives do not have these files unless produced with this option.

Some integration tests require the installed Sudachi dictionary and local
JMdict data. Candidate component provenance is retained in memory for eligible
candidates and supports some definition and parsing corrections; it does not
yet appear in exported CSVs.

The normal pipeline includes grammar cards by default; pass
`--no-grammar` to disable them. Rules create separate grammar candidates when
narrowly supported source examples are found. See the [grammar documentation](../grammar/README.md)
for the individual rules and their supported forms. If Anki is enabled, each
card is included in its normal preview and confirmation workflow. Existing
notes with those exact identities are protected from pruning; the plan refreshes
their meanings, frequencies and scores while preserving other fields. Existing
suffix-style grammar notes are renamed in place through the
Anki plan when the new identity is unambiguous; conflicts are retained for review. Grammar cards use the same scoring formula as vocabulary, including
source position, frequency and source diversity; they are added after lexical
filtering and do not use its minimum-frequency threshold. Grammar and lexical
cards are scored together, and eligible source-aligned grammar occurrences use
the same sentence ranking, deduplication and fallback as lexical examples. Recognized grammar spans
are reserved before lexical merging and collection, unless the existing dictionary
merger identifies a complete expression spanning the construction and its attached
word; that occurrence is then counted as the expression. Other occurrences and
eligible attached words remain lexical. See the point specifications for
construction-specific precedence and reservation. This can change lexical frequencies and may
place absent lexical notes in an Anki deletion plan when pruning is enabled.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [next steps](../next-steps.md) for current priorities and the
[problematic-entry inventory](../problematic-entries.md) for frequency-based
review examples. The [review evaluation workflow](../review-evaluation.md)
describes how to sample examples and compare independently labeled predictions.
The [analyzer shadow benchmark](../analyzer-shadow-benchmark.md) compares
raw-source token spans without changing mining output.
See [Japanese parsing research](../japanese-parsing-research.md) for analyzer
and evaluation options, and the [pipeline improvement roadmap](../pipeline-improvement-roadmap.md)
for broader work.

Dictionary-backed multi-token expressions that meet the frequency cutoff but
fail the per-source coverage rule are retained in the review CSV. An inflected
expression below the cutoff is also retained for review when its component
evidence points to a sufficiently frequent dictionary-backed base expression.
Their source
examples can represent different senses, so they are not automatically added
to Anki and existing notes are protected from pruning. This review path can
retain additional expressions beyond the examples motivating it.

The lexical merger also looks up adjacent noun and verb tokens as a JMdict
expression with an omitted を, using the verb's dictionary form. The candidate
keeps the actual source surface and component evidence. Exact dictionary merges
take precedence and reserved grammar tokens are not crossed. These matches can
be ambiguous with literal object-verb usage; dictionary evidence alone does not
establish the intended sense.


## Pipeline performance measurements

See the [performance plan](../pipeline-performance-plan.md) for findings and
future work.

Set `[profiling] enabled = true` in `config.toml` to collect named phase
measurements in `performance.json`. Each executed step has a `total` in the
report and in `performance.json`, matching the duration in its existing
`steps/*.json` record. Total is elapsed wall time excluding recorded user
waits and includes cache handling. Named phases use elapsed wall time; they
can overlap or omit work, so they need not sum to the total. Failed steps may
have partial measurements. Phases not executed, such as matching on a grammar
cache hit, are absent rather than zero. Profiling is disabled by default.

Steps can use `with self.performance_phase("name"):` around work, or
`self.add_performance_time(name, seconds)` for accumulated intervals. Repeated
names accumulate; `total` is reserved. The grammar step currently records
alignment, matching, expression-precedence preparation, aggregation, and
cache key/read/write phases. Older profiling archives remain readable.

Use `python scripts/report_performance.py RUN_DIR [RUN_DIR ...]` to combine
named timings with stage and cache records. Compare runs only after checking
inputs, configuration, revision, dependencies, cache state and measurement
settings. Timer overhead has not yet been quantified.
