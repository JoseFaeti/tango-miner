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
an interrupted Anki apply can have made partial changes. Each executed stage also
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
and deleted. The run archive contains `anki-plan.json` with planned additions,
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
as usual. The cache keys the ordered normalized sentences and the tokenizer identity,
including tokenizer and cache source code, Python and SudachiPy versions,
Sudachi settings, JSON, and system and user dictionary contents. The source
dependency list in `TokenizeStep.py` must be updated if tokenizer or cache
logic gains another project helper. Dictionary hashing occurs once per tokenizer stage. The
versioned cache replaces the old pickle and timestamp index; old entries
are left on disk and are no longer read. Cache files contain source-derived
token text. Its restricted pickle reader rejects global objects. Tokenizer and merge
stage completion messages report cache hits and misses, refreshes when forced,
and when caching is disabled. Tokenizer counts are per file; merge counts are
per source group, with cached stream counts also shown.

### Merge cache

The merge stage caches results per source file in the application cache directory.
Keys include the raw token streams and source metadata, merge lookahead, JMdict
content hash, and a fingerprint of the merge stage, cache code, and their
shared project dependencies. Changes to those modules invalidate merge entries;
changes to unrelated stages such as Anki export do not. The dependency list in
`StageCache.py` must be updated if the merge stage gains a new project dependency.
Cached data includes source
text and paths; old entries remain until manually removed from the application's
`stages/merge-v2` cache directory. Older `merge-v1` entries are no longer used
and can be removed separately. Cache files are JSON and are replaced atomically.
A missing dictionary hash or occurrence diagnostics disables merge caching for
that run so rejected-merge diagnostics are complete. The run summary records
merge hits, misses, refreshes, write failures and any reason for disabling it.
Hit, miss and
refresh counts refer to source groups; separate stream counts show how many
token streams each category covers. Files with no token streams do not form
a group.

### Grammar cache

The grammar analysis stage caches its results for the complete ordered corpus in
`stages/grammar-v1`. The key includes token streams and source metadata, grammar
rules and their project dependencies, example limit, expression preference,
merge lookahead, and the JMdict content hash when expression preference is enabled.
A change to any input invalidates the corpus entry. The cache restores matches,
spans and examples used by later stages; custom rule sets and an unavailable
required dictionary hash disable caching. Cached JSON contains source text and
paths. Entries are checksummed and replaced atomically, and invalid entries are
recomputed. The grammar stage report records hits, misses, refreshes, write
failures, and any reason caching was disabled. Update the grammar dependency
fingerprint in `GrammarCache.py` when adding project dependencies.

Use `--refresh-cache merge` to recompute and replace merge entries. Use
`--refresh-cache all` to refresh grammar and merge entries and force retokenization. These
options refresh rather than delete older entries. Tokenizer cache entries are keyed by normalized sentence content and tokenizer
dependencies. Other stages run normally.

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

For other JSONL diagnostics, search a field or the whole record without loading
the file into memory or running the pipeline:

```sh
python -X utf8 scripts/query_jsonl.py runs/example-run/occurrences.jsonl --field sentence --contains ないし --show candidate --show sentence
python -X utf8 scripts/query_jsonl.py runs/example-run/grammar-shadow.jsonl --field proposals.construction_id --exact te_iru --show normalized_sentence --show proposals --limit 5
```

Dotted fields traverse arrays; repeated `--show` selects fields. Output is
bounded by `--limit` and `--width`, but the whole file is scanned to count
matches. These records can include source text and paths.

To check a corpus with Anki disabled and save the verbose progress to a log:

```sh
python -X utf8 scripts/check_corpus.py --input path/to/corpus --output-dir checks --baseline runs/example-run --word する
```

Use an actual completed run directory for `--baseline`; omit it to skip the
comparison. Pass `--no-grammar` to omit grammar cards from the check. The wrapper
loads the normal configuration, forces Anki off in a
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
offline checks. See [grammar shadow prototype](docs/grammar-shadow-prototype.md).

The normal pipeline includes experimental grammar cards by default; pass
`--no-grammar` to disable them. The rules create separate
`〜ている`, explanatory `〜んだ` and desire
`〜たい`, `〜てしまう`, concessive
`〜ものの`, and `〜てくる` candidates when
narrowly supported source examples are found. If Anki is enabled, each
card is included in its normal preview and confirmation workflow. Existing
notes with those exact identities are retained without automatic updates or
pruning. Existing suffix-style grammar notes are renamed in place through the
Anki plan when the new identity is unambiguous; conflicts are retained for review. Grammar cards use the same scoring formula as vocabulary, including
source position, frequency and source diversity; they are added after lexical
filtering and do not use its minimum-frequency threshold. See the [grammar shadow prototype](docs/grammar-shadow-prototype.md)
for supported forms, limitations, and note behavior. Recognized grammar spans
are reserved before lexical merging and collection, unless the existing dictionary
merger identifies a complete expression spanning the construction and its attached
word; that occurrence is then counted as the expression. Other occurrences and the
preceding verb remain eligible. This can change lexical frequencies and may
place absent lexical notes in an Anki deletion plan when pruning is enabled.
Existing filters, including the standalone proper-noun filter, do not fully
match the recall-first goal and should be evaluated before changing them.

See [next steps](docs/next-steps.md) for current priorities and the
[problematic-entry inventory](docs/problematic-entries.md) for frequency-based
review examples. The [review evaluation workflow](docs/review-evaluation.md)
describes how to sample examples and compare independently labeled predictions.
The [analyzer shadow benchmark](docs/analyzer-shadow-benchmark.md) compares
raw-source token spans without changing mining output.
See [false-positive analysis](docs/false-positive-analysis.md) for the current
quality policy, known issues, and next steps, [Japanese parsing research](docs/japanese-parsing-research.md)
for analyzer and evaluation options, and the [pipeline improvement
roadmap](docs/pipeline-improvement-roadmap.md) for broader work. The
[initial audit](docs/anki-output-audit-2026-10-26.md) and
[re-audit](docs/anki-output-reaudit-2026-10-26.md) record output evidence.

Dictionary-backed multi-token expressions that meet the frequency cutoff but
fail the per-source coverage rule are retained in the review CSV. An inflected
expression below the cutoff is also retained for review when its component
evidence points to a sufficiently frequent dictionary-backed base expression.
Their source
examples can represent different senses, so they are not automatically added
to Anki and existing notes are protected from pruning. This review path can
retain additional expressions beyond the examples motivating it.

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
with sentence-initial `しかし、` use that source context as evidence for the
conjunction; other occurrences do not supply its examples. The other uses are
still included in the total frequency and do not get a separate review card.
Occurrence diagnostics capture raw components before this contextual cue is
added during aggregation. Cases
without component evidence retain the existing frequency-based lookup. Short
nouns and other unresolved analyses remain for review. Source-grounded potential forms may use a
labeled definition derived from an unambiguous matching base verb when the
full potential form occurs in the source.
