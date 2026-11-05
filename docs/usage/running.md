# Running

The console updates a single progress line for the current stage, including its
elapsed wall time and position in the pipeline, for example `[01:23 4/18]`.
The elapsed time updates when progress is reported. Use `--verbose` (or `-v`) to keep completed stage
progress and messages in the output. Messages printed by the stages themselves,
including Anki previews and prompts, remain visible in either mode. Every run
also writes stage start and completion messages and step output to `pipeline.log`
in its run archive, including on failure. Live percentage updates are omitted.
The log can contain source-derived text and paths.

For unattended runs, pass `--silent`. It suppresses pipeline progress, step
output, notifications and completion messages on the console; inspect the run's
`pipeline.log` and process exit status after completion. Pipeline exceptions and
their tracebacks are recorded in the log. Argument, configuration, or startup
errors before the archive exists are still reported on standard error. Silent
mode requires Anki disabled or output-only, so it cannot wait for confirmation.
Use this mode for pipeline runs initiated by scripts and agents.

[Contents](../usage-reference.md)


The executable is `tango_miner.py`. Run `python tango_miner.py --help` for its
current options. For example, from the repository root:

```sh
python tango_miner.py --input path/to/text-directory --output path/to/output.csv
```

Set the agreed full corpus directory in the Git-ignored `.env` at the project
root, using an absolute path (quotes are optional):

```dotenv
TANGO_INPUT_PATH="path/to/agreed-corpus"
```

Replace the example with the absolute path on your machine. This file is private
local configuration; do not commit it. Update the value when the agreed corpus
changes. Run `./scripts/run_full_corpus.ps1` from the project root in
PowerShell. It reads that value, uses recursive discovery and silent mode, and checks that Anki
is disabled or output-only. Save the wrapper output to a log and wait for the
process to finish before inspecting the log or run artifacts; check its exit
status and report failures. Logs and archives can contain source text and paths.

The main `tango_miner.py` command also uses `TANGO_INPUT_PATH` from `.env`
when neither `--input` nor `[input] path` is set. Explicit command-line input
takes precedence over the configuration file, which takes precedence over `.env`.
The main command still uses its configured recursion and Anki settings; check
`--show-config` before a full run.

## Agent full-corpus procedure

Obtain explicit user authorization before starting a full corpus run. Use the
curated input corpus agreed with the user and configured in the private `.env`;
source files may be outside the project directory. Verify the resolved
configuration has Anki disabled or in output-only mode; do not mutate Anki.
Save verbose output to a log and wait for the process completion signal or
exit status before inspecting the run, its log, or its artifacts. Then use
bounded inspection tools, report failures and Anki status, and distinguish
observed counts from independently measured precision or recall.

The pipeline needs Python, SudachiPy with its full dictionary and the local
Sudachi configuration, `appdirs`, and a JMdict XML file. The dictionary path is
configured in `config.toml`, as are the Sudachi config and dictionary; some
resources are local and ignored by Git. The current baseline uses Sudachi
with the full system dictionary and an empty `userDict` in
`sudachi.config.json`; JMdict is still used for merging and definitions. The
previously generated user dictionary is not part of the baseline.
For the tested JMdict snapshot and a hash check, see
[development setup](development.md#jmdict-test-baseline). Other dependencies
still need a reproducible setup specification. Check the current configuration
before a run.

Each completed or failed pipeline run writes `report.md`, a concise overview of available candidate counts, grammar occurrences, Anki status and plan, and the five slowest stages. Read it first; use `summary.json`, stage records and other detailed artifacts when investigating. Counts at the definition stage precede grammar card addition. Grammar rule occurrences can overlap. Planned Anki changes are not necessarily applied changes. Timings exclude recorded user wait and are not directly comparable without checking inputs, configuration and cache state. An interrupted process may not write a final report.

Each run writes the requested CSV and its `.dropped.csv` and `.sentence.csv`
companions under `runs/<local-date>_<time>_<git-revision>/` beside the
requested output path. Repeated runs in the same minute receive a numbered
suffix rather than overwriting earlier files. The run also saves the resolved
`config.json`, `notes.txt`, and an `input-manifest.json` with SHA-256 hashes
of discovered input files. `summary.json` records run status, per-stage counts
and durations, file-level tokenizer cache hits and misses, the retokenization
option, dependency versions and dictionary hashes, and the Anki plan and
outcome. It remains `running` if the process is terminated before cleanup;
an interrupted Anki apply can have made partial changes. After CSV output, the run writes `csv-changes.json` comparing valid candidates with the preceding completed archive in the same `runs` directory. Schema version 3 groups entries into `added`, `removed`, and `changed`; a candidate moving into or out of the valid CSV appears as an addition or removal. Added and removed entries contain their candidate fields directly. Example changes identify zero-based positions and contain the complete previous example and only changed properties of the new example; additions or removals use null for the absent side. Examples preserve highlight spans. The report records the baseline, input/config equality and counts; a first run reports `no_baseline`. Candidate text is the identity; renames appear as removal and addition. A malformed baseline fails the comparison step. The report contains source-derived examples and definitions, not sentence-level source metadata or Anki changes.

Each executed stage also
writes an ordered `steps/NN-StageName.json` report with its status, duration,
available input and output counts, completion message and links to related run
artifacts. Grammar reports include occurrence and example counts per rule;
cache and Anki reports include their recorded outcomes. A report is created
when a stage starts and updated on completion or failure, so an interrupted
stage may remain `running`. These are compact summaries, not full intermediate
data dumps; optional diagnostics provide more detail and contain source text.
Stages disabled by configuration have no report. Hashing large
dictionaries adds startup time. Git changes, including untracked files, are saved
in `changes.patch` when the checkout is dirty. Ignored files (including local
dictionaries) are not included, so their versions should be recorded in notes
when needed. If Git is unavailable, the folder uses `unknown` and notes the
error. Recognized run archives are excluded from recursive input discovery even if
the output location changes; other input folders named `runs` remain eligible.

The
Anki export is disabled by default. Setting `[anki] enabled = true` in
`config.toml` enables the AnkiConnect step after CSV output. It reads the deck
and reports how many notes would be added, modified (including tag changes),
and deleted. Fetching notes, preparing the comparison, and comparing notes each
show their own progress before the confirmation prompt. Action preparation and
application are shown separately after approval. The run archive contains `anki-plan.json` with planned additions,
field and tag changes, and deletions; it is written before confirmation and
contains deck content. The preview still compares notes and checks additions with
Anki; mutation request payloads are prepared only after approval. The plan
describes intended operations, while
`summary.json` records the outcome; a failure during application can leave
partial changes. `[anki] output_only = true` (the default) stops after this preview.
Set it to `false` to allow changes; the step then asks for explicit confirmation
before creating a model or changing notes. Declining or unavailable input leaves
Anki unchanged. Absent notes are retained by default and listed in the plan as `retained_absent`.
Set `[anki] prune = true` to include absent notes in the deletion plan; approving
such a plan can remove notes when the corpus is partial.

