# Pipeline improvement roadmap

## Purpose

This document records the most valuable next improvements to the mining
pipeline.  It is based on the current executable path in `tango_miner.py`:

```text
discover files -> read -> extract sentences -> normalize -> raw Sudachi tokens
-> JMdict compound merge -> build word statistics -> frequency filter
-> definitions -> score -> attach examples -> CSV -> AnkiConnect
```

The recommendations preserve the project's current **recall-first** policy.
Diagnostics should make a questionable candidate explainable; they should not
silently add broad exclusion rules that hide grammar, colloquialisms, or
source-specific vocabulary.

## Highest-priority work

### 1. Introduce a versioned run configuration

**Status (2026-10-27): partial.** Version 1 resolves input path, recursion,
encoding, file extensions, Sudachi and JMdict settings, compound lookahead,
frequency thresholds, scoring,
example ranking, output
path, debug mode, and Anki
enablement, connection, destination, and batch settings from defaults, TOML,
and explicit CLI options. The editable `config.toml` is loaded by
default, and `--show-config` prints the resolved settings. The
remaining stage-specific settings, generated defaults, and diagnostic-run copy
are future work; they are not represented as configurable until wired in.

Runtime policy is presently distributed across module constants and CLI code:
minimum frequency in `tango_miner.py`, file extensions in
`GatherInputFilesStep`, tokenizer settings in `TokenizeStep`, sentence limits
and heuristics in several steps, scoring weights in `ScoreWordStep`, example
ranking constants in `AttachSentencesStep`, and Anki details in
`AddWordsToAnkiStep`.  This makes a run hard to reproduce or tune without
editing source.

Add a typed `RunConfig` (dataclasses are sufficient) and load it from a TOML
file.  TOML is a good fit because Python has `tomllib` in current supported
versions and it remains pleasant to edit by hand.  Keep safe defaults in code,
then apply this precedence order:

1. built-in defaults;
2. an optional config file;
3. explicit command-line options.

Suggested top-level sections:

| Section | Owns |
| --- | --- |
| `input` | extensions, recursion, text encoding, tag extraction rule |
| `extraction` | mode thresholds, length and Japanese-script constraints |
| `tokenizer` | Sudachi config/dictionary, split mode, cache directory and cache policy |
| `merge` | JMdict path and compound lookahead |
| `filter` | minimum frequency, per-source threshold, percentile policy |
| `scoring` | frequency/index weights and tag-diversity parameters |
| `sentences` | number of examples and ranking/penalty values |
| `dictionary` | JMdict path and definition-selection policy |
| `output` | output directory/name, CSV columns, overwrite policy |
| `anki` | enabled state, endpoint, deck, model, sync/deletion policy |
| `debug` | artifact directory, selected stages, verbosity, retained temporary files |

Document every setting in `config.toml`, and write a
copy of the fully resolved configuration into each diagnostic run.  Include a
`config_version` so future semantic changes can be migrated deliberately.

### 2. Make side effects explicit and safe

The Anki step now previews counts and requires confirmation before changes;
`[anki] prune = true` separately opts in to deletion of absent notes, which are
otherwise listed as retained in the detailed plan.
`[anki] output_only = true` prevents changes altogether. The pipeline runs
`AddWordsToAnkiStep` after CSV output only when
`[anki] enabled = true` in the configuration. With pruning enabled, that step also removes notes
that are no longer in the current result.  A partial
input selection, altered filter setting, or failed earlier run can therefore
affect an existing deck much more broadly than the user expects.

Change the default run mode to CSV-only or dry-run.  Require an explicit
`--anki` to contact AnkiConnect, and separate the destructive behavior behind
an explicit `--anki-prune` (or config equivalent).  Before any mutation,
produce and display a plan: notes to add, update, tag-change, and delete.
Support `--dry-run` for the full pipeline, including an Anki diff report.

The Anki step prints counts and saves `anki-plan.json` before confirmation,
including additions, deleted expressions, and before/after field and tag changes.
The plan contains deck content. `summary.json` records the outcome separately;
a failure during application may leave partial changes, and the plan is not a
per-operation receipt. Output-only mode writes the plan without changing Anki.

The Anki configuration should include endpoint, timeout, deck/model names, and
batch sizes rather than keeping them as module constants.  Validate the model
fields before export and fail with an actionable message if the configured
model is incompatible.

### 3. Add structured, per-stage diagnostic artifacts

`--debug` currently enables console printing, writes two sentence dumps through
`DumpSentences`, and writes invalid dictionary words to an application cache
directory.  These outputs are split across locations and cannot explain why a
specific final card was kept, dropped, merged, or assigned an example.

Replace this with a single run directory, for example
`debug/<timestamp>-<run-id>/`, selected by `--debug-dir`.  A debug run should
write UTF-8, machine-readable files with stable names; large stages can use
JSON Lines or CSV rather than one enormous JSON object.

| File | What it should answer |
| --- | --- |
| `manifest.json` | input paths/hashes, tool/config versions, timestamps, Python/Sudachi/JMdict versions, and stage durations/counts |
| `01-input-files.csv` | which files were accepted or skipped, their tags, sizes, and read errors |
| `02-extraction.jsonl` | extraction mode, raw line count, extracted sentence count, and rejected/changed text reasons per source |
| `03-normalization.jsonl` | original and normalized sentence plus an explicit transform/rejection reason |
| `04-raw-tokens.jsonl` | sentence ID, source, raw Sudachi token fields, and cache hit/miss |
| `05-merged-tokens.jsonl` | token sequence before/after merge and the matched JMdict entry/merge decision |
| `06-candidates.csv` | lemma, surface forms, frequency, source tags, position, POS, and whether it passed each filter |
| `07-definitions.csv` | selected entry/sense, alternative entry count, definition presence, and invalid reason |
| `08-scores.csv` | raw score components, normalization factor, and final score |
| `09-sentence-ranking.jsonl` | candidate example fitness components, deduplication decisions, selected examples, and fallback use |
| `10-anki-plan.json` | planned adds, updates, tag changes, deletes, and requests/responses with note IDs redacted as appropriate |

Assign stable identifiers to source files, sentences, and candidates so those
files can be joined.  Do not log only a rendered message such as “filtered”;
record the comparison values and named reason (`frequency_below_threshold`,
`insufficient_source_coverage`, `no_dictionary_entry`, and so on).

Use Python's `logging` module instead of the mutable global `print_debug`.
Console logs should stay concise and progress-oriented; the artifact directory
should contain detailed `run.log` plus structured evidence.  Add `--log-level`
and ensure sensitive local paths can be omitted or relativized.

### 4. Preserve provenance through every stage

The pipeline carries source origin and token-aligned spans into selected example
sentences. If alignment fails, it avoids guessing a highlight position. More
provenance is still needed to explain every candidate occurrence.

Make source path/ID, raw sentence text, normalized text, extraction mode, and
token offsets first-class metadata that travels with the artifact.  Merged tokens now retain component surfaces, lemmas, base forms, readings,
and POS; eligible candidates count distinct component sequences. This does not
retain per-occurrence source links or the matched dictionary entry, expose the
evidence in output, or change filtering. Preserve those missing details next.  For a
final word, retain the source occurrences used to calculate frequency and
index.  This supports both the diagnostic artifacts above and future
context-sensitive definitions without sacrificing recall.

## Command-line interface

Retain existing options as aliases where possible and use conventional
kebab-case names for new options (for example, keep `--minFrequency` as a
deprecated alias for `--min-frequency`).  Useful additions are:

| Option | Result |
| --- | --- |
| `--config PATH` | load TOML configuration |
| `--write-default-config PATH` | write documented defaults and exit |
| `--show-config` | print resolved configuration and exit |
| `--min-frequency N` | override frequency threshold |
| `--include EXT` / `--exclude GLOB` | refine input discovery without source edits |
| `--encoding NAME` / `--on-decode-error {fail,skip,replace}` | control file reading and report failures |
| `--tag-from {filename,parent,none}` | make tag derivation explicit rather than hard-coding bracketed filenames |
| `--no-cache`, `--cache-dir PATH`, `--clear-cache` | control token cache behavior |
| `--stop-after STAGE` / `--start-at STAGE` | debug or resume a pipeline boundary safely |
| `--debug-dir PATH`, `--debug-stage STAGE`, `--log-level LEVEL` | control observability without mixing output locations |
| `--dry-run` | compute outputs and plans without writing CSV/Anki mutations |
| `--anki`, `--anki-prune`, `--anki-deck NAME`, `--anki-model NAME`, `--anki-url URL` | opt in to and configure Anki export |
| `--overwrite` | acknowledge replacement of an existing output file |
| `--summary-json PATH` | write a compact automation-friendly run summary |

Validate paths and option combinations before expensive dictionary loading.  A
single `--verbose` should never be interpreted as permission to modify Anki.

## Pipeline-specific quality improvements

### Input and sentence preparation

- Report unsupported files and decode failures individually instead of only
  reporting the final count.
- Sort discovered input paths for deterministic corpus ordering and deterministic
  output.  The current directory iterator depends on filesystem order.
- Separate document extraction by type.  Merely reading `.pdf`, `.html`, CSV,
  and XML as UTF-8 text is unreliable; dispatch to explicit extractors or
  reject unsupported binary formats with a clear message.
- Record which extraction heuristic was chosen for each file and add fixtures
  for dialogue, hard-wrapped prose, subtitle, HTML, and noisy-game-script
  inputs.
- Make normalizer decisions observable: minimum length, hiragana requirement,
  bracket repair, whitespace removal, and character cleanup can otherwise make
  useful source text disappear without evidence.

### Tokenization and dictionary work

The baseline is Sudachi with its full system dictionary and no user dictionary;
JMdict remains the source for compound merging and definitions. The old user
dictionary was generated from JMdict hits over sentence substrings with shared
Sudachi entry parameters. A dictionary hit does not establish a contextual
word boundary, so do not restore that dictionary as a general tokenization fix.
Revalidate historical findings with the new baseline before deciding whether
an old issue remains or needs repair. See the
[run comparison and issue tracker](false-positive-analysis.md#no-user-dictionary-baseline-2026-10-30).

- Configure and report the Sudachi dictionary path, split mode, and tokenizer
  fingerprint.  The cache fingerprint should derive from all tokenization-
  relevant settings and dependency versions, rather than require a manually
  edited date string.
- Replace pickle cache files with a safer documented format where practical, or
  make the cache explicitly local/trusted.  Never load a cache from an
  untrusted directory.
- Count raw-token cache hits/misses and expose them in the manifest.
- Make `max_lookahead`, kana-only compound rules, and JMdict selection policy
  configuration values with regression-test fixtures.  Keep recording rejected
  merge candidates so false positives can be investigated.
- Improve definition selection using the source POS, surface form, conjugation,
  and example context.  The current “most common definition” is a known
  quality bottleneck; return confidence and alternatives rather than pretending
  an ambiguous selection is certain.

### Candidate filtering, scoring, and examples

- Name and configure the current percentile-based threshold (`keep_percent`) and
  source-coverage rule (`frequency / tag count`).  Both affect output but are
  not exposed by the CLI.
- Emit a decision record for every discarded candidate.  This supports tuning
  without changing recall policy blindly.
- Parameterize score weights and write all pre-normalization components.  Add
  corpus-level score distributions to catch surprising ranking shifts.
- Fix the sentence-attachment completion count: the current expression iterates
  over word strings rather than `WordStats` values, so it does not represent
  attached sentence total.
- Make sentence ranking explainable at per-example level and distinguish the
  primary scorer from the fallback path.  Track the percentage of cards using
  fallback examples as a quality signal.
- Consider diversity constraints (different sources/tags and no near-duplicate
  sentences) once the trace data shows repeated examples are a material issue.

### Output contract

- Write column headers and document the stable CSV schema.  Output consumers
  should not need to infer the meaning/order of nine positional fields.
- Generate output atomically (write a temporary sibling file then rename) so an
  interruption cannot leave a half-written deck import.
- Give the kept, dropped, and sentence files a shared run identifier and write
  a compact `summary.json` next to them.
- Use an explicit CSV dialect and retain source IDs/paths, candidate IDs,
  filter status, and definition confidence in an extended/audit export.  Keep a
  simple import-compatible output as a separate profile if needed.

## Engineering foundation

1. Establish a small `PipelineContext` containing the resolved config, logger,
   run directory, metrics collector, and dependency handles.  Pass it to steps
   instead of relying on module globals and ad hoc constructor constants.
2. Give each step a declared input/output type and a stable stage name.  A
   `StageResult`/artifact envelope can carry data, provenance, metrics, and
   diagnostics without repeatedly rebuilding bare `Artifact` objects.
3. Define failure policy per stage: a bad input file may be skippable, an absent
   JMdict should fail before processing, and an Anki export failure should leave
   the CSV and its manifest intact while returning a non-zero exit code.
4. Add an end-of-run summary: files accepted/skipped, sentences extracted and
   rejected, cache statistics, raw/kept/dropped candidates, definitions found,
   example coverage, output paths, and Anki plan/result.
5. Pin/package dependencies and add a reproducible development setup.  A fresh
   checkout should state Python version, Sudachi dictionary requirements,
   JMdict location, and test command.

## Test plan and implementation order

Implement in small vertical slices, keeping existing behavior as the default
until a deliberate compatibility change is released.

1. Add `RunConfig`, an editable default TOML, config precedence tests, and a
   `--show-config` smoke test.
2. Introduce `PipelineContext`, structured logging, manifest, stage timing, and
   deterministic input ordering.  Add unit tests that assert counts and paths.
3. Preserve provenance (especially `SegmentedSentence.origin`) and add compact
   debug artifacts for extraction, merges, candidate decisions, and sentence
   ranking.  Test schema and stable IDs rather than only text snapshots.
4. Make output atomic and self-describing; add CSV schema/overwrite tests.
5. Put Anki behind explicit opt-in, implement dry-run/plan mode, then test the
   plan with a fake AnkiConnect server.  Only after this should pruning be
   enabled explicitly.
6. Use the new diagnostics to improve contextual definition selection and
   candidate/example ranking.  Add regression fixtures for every confirmed
   false positive or valuable recall case.

Success means a user can rerun a corpus months later with the same configuration
and explain each resulting card—from source sentence through token/merge/filter
decisions to its definition, score, example, CSV row, and optional Anki change.
