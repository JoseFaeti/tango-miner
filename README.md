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

The output writer creates the requested CSV plus `.dropped.csv` and
`.sentence.csv` companions, overwriting existing files at those paths. The
Anki export is disabled by default. Setting `[anki] enabled = true` in
`config.toml` enables the existing AnkiConnect export after CSV output. It adds
and updates notes, changes tags, and deletes notes in the configured deck and
model that are absent from the current results. A partial corpus can therefore
delete existing notes; review the input and settings before enabling it.

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

Some integration tests require the installed Sudachi dictionary and local
JMdict data. Candidate component provenance is retained in memory for eligible
candidates; it does not yet alter selection or appear in exported CSVs.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [false-positive analysis](docs/false-positive-analysis.md) for the current
quality policy, known issues, and next steps, and the [pipeline improvement
roadmap](docs/pipeline-improvement-roadmap.md) for broader work. The
[initial audit](docs/anki-output-audit-2026-10-26.md) and
[re-audit](docs/anki-output-reaudit-2026-10-26.md) record output evidence.
