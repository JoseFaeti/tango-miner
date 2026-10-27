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
and deleted. `[anki] output_only = true` (the default) stops after this preview.
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

Some integration tests require the installed Sudachi dictionary and local
JMdict data. Candidate component provenance is retained in memory for eligible
candidates and supports some definition and parsing corrections; it does not
yet appear in exported CSVs.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [false-positive analysis](docs/false-positive-analysis.md) for the current
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
Short nouns, other uncertain analyses and ambiguous homophones remain for review instead of receiving an
unsupported definition. Source-grounded potential forms may use a
labeled definition derived from an unambiguous matching base verb when the
full potential form occurs in the source.
