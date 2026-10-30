# Highest-priority work

[Contents](../pipeline-improvement-roadmap.md)


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

