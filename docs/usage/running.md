# Running

The console updates a single progress line for the current stage, including its
elapsed wall time and position in the pipeline, for example `[01:23 4/18]`.
The elapsed time updates when progress is reported. Use `--verbose` (or `-v`) to keep completed stage
progress and messages in the output. Messages printed by the stages themselves,
including Anki previews and prompts, remain visible in either mode. Every run
also writes stage start and completion messages and step output to `pipeline.log`
in its run archive, including on failure. Live percentage updates are omitted.
The log can contain source-derived text and paths.

[Contents](../usage-reference.md)


The executable is `tango_miner.py`. Run `python tango_miner.py --help` for its
current options. For example, from the repository root:

```sh
python tango_miner.py --input path/to/text-directory --output path/to/output.csv
```

The pipeline needs Python, SudachiPy with its full dictionary and the local
Sudachi configuration, `appdirs`, and a JMdict XML file. The dictionary path is
configured in `config.toml`, as are the Sudachi config and dictionary; some
resources are local and ignored by Git. The current baseline uses Sudachi
with the full system dictionary and an empty `userDict` in
`sudachi.config.json`; JMdict is still used for merging and definitions. The
previously generated user dictionary is not part of the baseline. See
[baseline findings and follow-up](../false-positive-analysis.md#no-user-dictionary-baseline-2026-10-30).
A reproducible dependency and setup specification is still needed. Check the
current configuration before a run.

Each run writes the requested CSV and its `.dropped.csv` and `.sentence.csv`
companions under `runs/<local-date>_<time>_<git-revision>/` beside the
requested output path. Repeated runs in the same minute receive a numbered
suffix rather than overwriting earlier files. The run also saves the resolved
`config.json`, `notes.txt`, and an `input-manifest.json` with SHA-256 hashes
of discovered input files. `summary.json` records run status, per-stage counts
and durations, file-level tokenizer cache hits and misses, the retokenization
option, dependency versions and dictionary hashes, and the Anki plan and
outcome. It remains `running` if the process is terminated before cleanup;
an interrupted Anki apply can have made partial changes. After CSV output, the run writes `csv-changes.json` with all added, removed and changed candidate fields compared with the preceding completed archive in the same `runs` directory. The `changes` object groups entries into `added`, `removed`, and `changed` arrays; status transitions appear in `changed` alongside any other field changes. Schema version 2 identifies this grouped format. It records the baseline, input/config equality and counts; a first run reports `no_baseline`. The comparison covers the main and dropped/review CSVs, not sentence-level source metadata or Anki changes. Candidate text is the identity; renames appear as removal and addition. A malformed baseline fails the comparison step. The report contains source-derived examples and definitions.

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

