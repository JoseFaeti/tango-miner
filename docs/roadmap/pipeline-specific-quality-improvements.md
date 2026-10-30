# Pipeline-specific quality improvements

[Contents](../pipeline-improvement-roadmap.md)


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
[run comparison and issue tracker](../false-positive-analysis.md#no-user-dictionary-baseline-2026-10-30).

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

