# Tango Miner

Tango Miner mines Japanese learning candidates from material the learner intends
to read. It extracts and normalizes sentences, tokenizes them with Sudachi,
merges dictionary-backed expressions, aggregates and filters candidates, adds
JMdict definitions and example sentences, scores them, and writes CSV files for
review or Anki workflows.

The project prioritizes **recall**: vocabulary, grammar, fixed expressions,
colloquialisms, names, and uncertain cases can all be useful learning material.
A false positive is a demonstrably misleading target or definition, not simply
an unusual expression. Improve questionable analyses with source and POS
context; do not treat suspended cards as a blacklist or add broad exclusions
without assessing lost learning material.

## Running

The executable is `tango_miner.py`. Run `python tango_miner.py --help` for its
current options. For example, from the repository root:

```sh
python tango_miner.py --input path/to/text-directory --output path/to/output.csv
```

The pipeline needs Python, SudachiPy with its full dictionary and the local
Sudachi configuration, `appdirs`, and a JMdict XML file. The dictionary path is
configured in `config.toml`, as are the Sudachi config and dictionary; some
resources are local and ignored by Git. A reproducible dependency and setup
specification is still needed. Check the current configuration before a run.

Each run writes the requested CSV and its `.dropped.csv` and `.sentence.csv`
companions under `runs/<local-date>_<time>_<git-revision>/` beside the
requested output path. Repeated runs in the same minute receive a numbered
suffix rather than overwriting earlier files. The run also saves the resolved
`config.json`, `notes.txt`, and an `input-manifest.json` with SHA-256 hashes
of discovered input files. `summary.json` records run status, per-stage counts
and durations, file-level tokenizer cache hits and misses, the retokenization
option, dependency versions and dictionary hashes, and the Anki plan and
outcome. It remains `running` if the process is terminated before cleanup;
an interrupted Anki apply can have made partial changes. Hashing large
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
and deleted. The run archive contains `anki-plan.json` with planned additions,
field and tag changes, and deletions; it is written before confirmation and
contains deck content. The plan describes intended operations, while
`summary.json` records the outcome; a failure during application can leave
partial changes. `[anki] output_only = true` (the default) stops after this preview.
Set it to `false` to allow changes; the step then asks for explicit confirmation
before creating a model or changing notes. Declining or unavailable input leaves
Anki unchanged. Notes in the configured deck and model that are absent from the
current results are deleted when approved, so a partial corpus can remove notes.

## Run configuration

Edit `config.toml` to configure normal runs. It is loaded automatically
from the project directory; use `--config PATH` to select a different file. The
version 1 configuration currently covers input path, recursion, encoding and file
extensions; Sudachi and JMdict settings; compound merge lookahead; frequency thresholds;
scoring and example ranking; output path; debug mode; and Anki export, connection, destination, and batch settings. Built-in
defaults are used
first, then the file, then explicitly supplied CLI options. Use `--show-config`
to print the resolved configuration as JSON without running the pipeline. For
example, `python tango_miner.py --min-frequency 3 --show-config`. The existing `--minFrequency` spelling remains an alias.
Other pipeline constants are not configurable yet. Relative paths are resolved
against the current working directory.

Pass `--retokenize` to a normal pipeline run to regenerate tokenizer output
instead of reading cached tokens. The refreshed tokens are saved to the cache
for later runs. All other configured stages, including Anki if enabled, run
as usual. The cache fingerprint includes the Sudachi settings, JSON contents,
and SudachiPy version. Replacing dictionary files in place is not detected;
use `--retokenize` after such a change.

## Development

Run the test suite from the repository root:

```sh
python -m unittest discover -s tests
```

Compare the two most recent completed run archives without rerunning the
pipeline or contacting Anki:

```sh
python -X utf8 scripts/compare_runs.py --word しれる --word かもしれない
```

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

To check a corpus with Anki disabled and save the verbose progress to a log:

```sh
python -X utf8 scripts/check_corpus.py --input path/to/corpus --output-dir checks --baseline runs/example-run --word する
```

Use an actual completed run directory for `--baseline`; omit it to skip the
comparison. The wrapper loads the normal configuration, forces Anki off in a
temporary resolved configuration, and writes the run archive and a uniquely
named log under the output directory. Use `--config` to select another config,
`--retokenize` to refresh the cache, or `--occurrence-diagnostics` when that
evidence is needed. The compact comparison reports config equality; disabling
Anki can itself make configurations differ. The wrapper still runs the full
pipeline and may take significant time and disk space, but does not print its
progress log into the conversation.

For detailed parsing investigation, add `--occurrence-diagnostics` to a run.
It writes `occurrences.jsonl`, `rejected-merges.jsonl`, and `candidate-diagnostics.jsonl` in the run
archive. These files can be very large and include source text and source
paths. Each occurrence records its normalized sentence, source, token position,
aligned character span (or null if alignment failed), candidate, raw component
evidence, collection eligibility, and the final token analysis rule. The
candidate file records final output status, frequency, reading and definition
for candidates surviving frequency filtering. Join on `candidate`; an eligible
occurrence missing from the candidate file did not survive that filter. Rejected merge records cover dictionary matches actually considered by the greedy merger, with the guard reason and source sentence; they do not include unmatched sequences or alternatives skipped after a successful merge. Candidate diagnostics include the selected JMdict entry sequence and forms when available. Derived potential definitions have no selected entry recorded. The analysis rule does not imply a confidence score. Existing
archives do not have these files unless produced with this option.

Some integration tests require the installed Sudachi dictionary and local
JMdict data. Candidate component provenance is retained in memory for eligible
candidates and supports some definition and parsing corrections; it does not
yet appear in exported CSVs.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [next steps](docs/next-steps.md) for current priorities and the
[problematic-entry inventory](docs/problematic-entries.md) for frequency-based
review examples. See [false-positive analysis](docs/false-positive-analysis.md) for the current
quality policy, known issues, and next steps, and the [pipeline improvement
roadmap](docs/pipeline-improvement-roadmap.md) for broader work. The
[initial audit](docs/anki-output-audit-2026-10-26.md) and
[re-audit](docs/anki-output-reaudit-2026-10-26.md) record output evidence.

## Unresolved definitions

Candidates without a dictionary definition are written to the existing
`.dropped.csv` review file, with their examples also marked `review` in the
sentence CSV. They are not classified as invalid solely because a definition
is missing. Existing Anki notes for those candidates are protected from
deletion and are not updated; new unresolved candidates are not added to Anki.
Candidates absent from the filtered results can still be included in the Anki
deletion plan. Short kana candidates can use unanimous retained token POS to
select a unique compatible JMdict entry for auxiliaries and pre-noun forms.
When a candidate has multiple dictionary entries, retained single-token POS,
reading and verb-conjugation evidence can identify a compatible entry. The
most frequent uniquely identified entry supplies the definition, and examples
are restricted to matching occurrences. Ties and analyses that cannot identify
an entry remain for review. Counts still include all occurrences; optional
candidate diagnostics report how many support the selected definition. Cases
without component evidence retain the existing frequency-based lookup. Short
nouns and other unresolved analyses remain for review. Source-grounded potential forms may use a
labeled definition derived from an unambiguous matching base verb when the
full potential form occurs in the source.
