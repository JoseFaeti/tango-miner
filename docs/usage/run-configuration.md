# Run configuration

[Contents](../usage-reference.md)


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

