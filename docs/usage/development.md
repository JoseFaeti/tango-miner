# Development

[Contents](../usage-reference.md)


Run the test suite from the repository root:

```sh
python -m unittest discover -s tests
```

Compare the two most recent completed run archives without rerunning the
pipeline or contacting Anki:

```sh
python -X utf8 scripts/compare_runs.py --word しれる --word かもしれない
```

Use `--json-output PATH` to save the complete structured comparison for any two runs.
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
python -X utf8 scripts/query_jsonl.py runs/example-run/grammar-shadow.jsonl --field proposals.construction_id --exact te_iru --show normalized_sentence --show proposals --limit 5
```

Dotted fields traverse arrays; repeated `--show` selects fields. Output is
bounded by `--limit` and `--width`, but the whole file is scanned to count
matches. These records can include source text and paths.

Full corpus checks require an agreed input corpus. Before running, verify the
resolved configuration disables Anki or uses output-only mode; save verbose
output to a log and wait for completion before inspecting artifacts. The
previously documented `scripts/check_corpus.py` wrapper is not present.

Replay a registered grammar rule against an existing shadow archive without
running the pipeline:

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

For experimental, review-only grammar proposals, use `--grammar-shadow`.
This writes `grammar-shadow.jsonl` in the run archive before token merging;
it does not itself alter candidate or Anki decisions; the separate normal grammar
stage remains enabled unless `--no-grammar` is passed. The file contains source-derived
text and paths. The rules are preliminary, all proposals remain ambiguous, and
the option does not disable Anki: use an Anki-disabled configuration for safe
offline checks. See [grammar shadow prototype](../grammar-shadow-prototype.md).

The normal pipeline includes experimental grammar cards by default; pass
`--no-grammar` to disable them. The rules create separate
`〜ている`, explanatory `〜んだ` and desire
`〜たい`, `〜てしまう`, concessive
`〜ものの`, `〜てくる`, `〜ていく`, `〜ない`, `〜たら`, and
`〜たことがある`, `〜ておく`, permission `〜てもいい`, prohibition `〜てはいけない`, obligation `〜なければならない`, occasional `〜ことがある`, `〜ように`, `〜と思う`, and `〜てみる` candidates when
narrowly supported source examples are found. If Anki is enabled, each
card is included in its normal preview and confirmation workflow. Existing
notes with those exact identities are retained without automatic updates or
pruning. Existing suffix-style grammar notes are renamed in place through the
Anki plan when the new identity is unambiguous; conflicts are retained for review. Grammar cards use the same scoring formula as vocabulary, including
source position, frequency and source diversity; they are added after lexical
filtering and do not use its minimum-frequency threshold. Grammar and lexical
cards are scored together, and eligible source-aligned grammar occurrences use
the same sentence ranking, deduplication and fallback as lexical examples. See the [grammar shadow prototype](../grammar-shadow-prototype.md)
for supported forms, limitations, and note behavior. Recognized grammar spans
are reserved before lexical merging and collection, unless the existing dictionary
merger identifies a complete expression spanning the construction and its attached
word; that occurrence is then counted as the expression. For `〜ておく`, a longer
JMdict expression beginning at `おく`, such as `おけばいい`, also takes precedence. Other occurrences and the
preceding verb remain eligible. For `〜と思う`, the matched 思う is reserved;
independent lexical uses remain eligible. This can change lexical frequencies and may
place absent lexical notes in an Anki deletion plan when pruning is enabled.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [next steps](../next-steps.md) for current priorities and the
[problematic-entry inventory](../problematic-entries.md) for frequency-based
review examples. The [review evaluation workflow](../review-evaluation.md)
describes how to sample examples and compare independently labeled predictions.
The [analyzer shadow benchmark](../analyzer-shadow-benchmark.md) compares
raw-source token spans without changing mining output.
See [false-positive analysis](../false-positive-analysis.md) for the current
quality policy, known issues, and next steps, [Japanese parsing research](../japanese-parsing-research.md)
for analyzer and evaluation options, and the [pipeline improvement
roadmap](../pipeline-improvement-roadmap.md) for broader work. The
[initial audit](../archived/anki-output-audit-2026-10-26.md) and
[re-audit](../archived/anki-output-reaudit-2026-10-26.md) record output evidence.

Dictionary-backed multi-token expressions that meet the frequency cutoff but
fail the per-source coverage rule are retained in the review CSV. An inflected
expression below the cutoff is also retained for review when its component
evidence points to a sufficiently frequent dictionary-backed base expression.
Their source
examples can represent different senses, so they are not automatically added
to Anki and existing notes are protected from pruning. This review path can
retain additional expressions beyond the examples motivating it.


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
