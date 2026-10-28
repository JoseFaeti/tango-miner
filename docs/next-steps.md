# Next steps

A short, prioritized working list. Keep useful vocabulary, grammar, names,
colloquialisms, and uncertain candidates in scope. See the
[roadmap](pipeline-improvement-roadmap.md) for context and the
[issue inventory](problematic-entries.md) for example frequencies.

- [ ] Add independently invalidatable pipeline caches to speed up repeated
  runs. This is feasible for deterministic intermediate results, but first
  use archived stage timings and cache hit rates to identify expensive stages
  and measure serialization, hashing, and disk costs on a representative
  corpus. Merge caching is implemented per source, with `--refresh-cache merge`
  and `--refresh-cache all`; diagnostic runs bypass it. The first full-corpus
  run exposed excessive serialization overhead, so the cache now stores only
  merge changes and reports both source-group and token-stream counts. A
  stage-only full-corpus check matched all 521,821 streams and measured 52.5 s
  uncached, 77.2 s to populate, and 37.0 s warm, with 88 MB of entries.
  Continue measuring end-to-end runs before expanding to another stage. The tokenizer cache
  still needs the correctness work described below. Prefer per-file caching
  through merge where practical, so changing
  one input does not invalidate the entire corpus; aggregation and later
  stages depend on the combined inputs.

  - Introduce stable stage IDs and versioned cache entries keyed by the full
    upstream result identity, relevant settings, stage and helper logic/schema
    versions, and external dependencies. Include input content and paths where
    they affect tags or provenance, extraction/normalization behavior,
    Sudachi settings and dictionary contents, and JMdict contents for merge and
    definition stages. Include frequency, scoring and example settings only
    where used. Changes in an upstream identity must invalidate dependent
    results automatically. A conservative code fingerprint or explicitly
    maintained stage versions must cover shared helpers; document the chosen
    mechanism and test it.
  - Replace or integrate the existing raw-token cache: its per-file mtime
    shortcut can miss changed content with a preserved timestamp and can reuse
    stale tokens after extraction or normalization changes; its fingerprint
    also does not detect dictionaries replaced in place. Key it from actual
    normalized sentence content and tokenizer dependencies, retaining
    `--retokenize` as a compatible force-refresh alias. Account for hashing
    cost and ensure cached tokens stay aligned with their sentences.
  - Provide a repeatable stage-specific refresh option and an all-stage
    refresh option. Refresh must bypass and replace the selected entries;
    downstream results must be recomputed or validated against the refreshed
    output identity, while unrelated upstream caches remain reusable. Define
    separately whether a cache-clear command deletes old entries, and make
    all-cache clearing include the existing tokenizer cache without touching
    run archives. Report hits, misses, refreshes and reasons in the run summary.
  - Cache complete, isolated snapshots of intermediate data, including source
    paths, tags, token components and evidence, review status, and sentence
    information. Later stages mutate candidate data, so do not share mutable
    cached objects with a running pipeline. Version and validate the format;
    treat corrupt or incompatible entries as misses, write atomically, handle
    concurrent runs, and avoid loading untrusted pickle data. Set a size or
    cleanup policy and document that caches may contain source text and paths.
  - Always rediscover inputs and write a fresh manifest, run archive, CSVs and
    requested diagnostics. Merge diagnostics include rejected matches, so a
    cache hit must replay equivalent diagnostics or recompute that work. Do
    not cache Anki plans or apply operations: read the live deck and regenerate
    the plan and confirmation on each enabled run. Debug output likewise needs
    to be regenerated for the current run.
  - Add cached-versus-uncached equivalence tests for candidates, definitions,
    examples, provenance and diagnostics, plus targeted tests for individual
    and all-stage refresh, upstream changes, configuration and dictionary
    changes, added/deleted files, corruption and interruption. Include useful
    recall-sensitive cases and verify that Anki is never mutated by tests.
    Roll out the highest-value stages first rather than assuming every stage
    benefits from caching.
- [x] Use token evidence for contextual dictionary selection across
  homophones, with `する`, `いる`, and `ようやく` as regressions. Match
  examples to the selected analysis and report mixed occurrence counts.
- [ ] Review the 995 net additional unresolved candidates in the full-corpus
  check; improve coverage without guessing same-POS homophones. Present or
  separately review alternative supported meanings and merged analyses. The
  split nominalizing `ん` + copula `だ` analysis now supports the explanatory
  `んだ` entry; other `んだ` analyses still need separate review.
- [ ] Design separate candidate and Anki note identities for grammar points
  and lexical entries that share a spelling, for example `ない` and
  `ない (grammar)`. Preserve the original source form separately from the
  display/identity label, and use occurrence evidence to assign definitions,
  examples, and frequency without guessing ambiguous analyses. Assess
  duplicate detection, existing-note migration and deletion plans, user edits,
  and compatibility with CSV export. Keep unsupported or mixed occurrences
  available for review and protect useful noun, adjective, and grammar uses.
- [ ] Improve verb and inflection mismatches using source and POS evidence.
  Context-specific repairs cover auxiliary `おって` after a continuative verb
  or verb + connective `て` / `で`, passive `覆われた` /
  `覆われて`, and object + `つれていく` / `つれてくる`; other contexts
  still need review. Preserve useful nouns and expressions.
- [ ] Consider a configuration option for potential forms: keep separate
  candidates or associate them with the base verb. Assess learning value,
  ambiguity, example selection, frequency accounting, and Anki effects;
  preserve useful potential-form evidence and avoid losing candidates.
- [ ] Separate expression and verb occurrences, starting with `にかけて`;
  preserve both the swearing expression and ordinary `掛ける` uses.
- [ ] Address `により` and investigate remaining boundary errors such as
  `ないし` and `つかる` without discarding valid occurrences.
- [ ] Recheck the archived `しれる`, `ことし`, `わる`, and `疲れた` examples
  against current code; distinguish fixed cases from remaining mixed contexts.
- [ ] Investigate standalone proper-noun filtering and improve name handling;
  add recall regressions before changing eligibility.
- [x] Improve occurrence diagnostics to capture rejected merges and selected
  dictionary entries; make mixed analyses and unresolved candidates easier to
  review. See [false-positive analysis](false-positive-analysis.md).
- [ ] Improve Anki safety and planning: separately opt in to pruning and track
  applied operations or partial failure. The detailed plan already exists; see
  the [roadmap](pipeline-improvement-roadmap.md).
