# Plan: a layered grammar-aware mining pipeline

Status: **proposal for review**, 2026-10-28. This replaces the priorities in
`sentence-analysis-plan.md` for the present work; that document remains as
historical context. No pipeline or Anki behavior changes are authorized by
this document alone.

## Goal and approach

Recognize grammar and expressions in the context of a sentence, while retaining
their constituent vocabulary and competing interpretations. Use morphology,
source-aligned construction patterns, dictionary evidence, and optionally a
contextual parser as complementary layers. Grammar matches are proposals, not
proof that a construction is being used. Preserve uncertain, colloquial,
unlisted, and proper-name candidates. Perfect interpretation is not assumed.

First build a small end-to-end shadow prototype for explanatory `んだ`,
`〜ている` and contracted variants, and concessive `ものの`. These exercise
multiple tokens, inflection, colloquial text, and ambiguity. Add controls for
nominal `もの` + `の`, non-explanatory `んだ`, and lexical uses of `いる`.
Decide exactly which variants to implement from observed token analyses; do not
claim to recognize variants that have not been tested.

## Sources and selection

Use the existing **JMdict and Sudachi** as the baseline for lexical entries,
inflection and component evidence. JMdict includes expression and grammatical
sense information, but is not a complete construction grammar and a matching
entry does not identify a meaning in context. Retain the selected entry and
sense restrictions as evidence, not as a substitute for a grammar analysis.
See [JMdict documentation](https://www.edrdg.org/jmwsgi/edhelp.py?svc=jmdict).

Evaluate **[OpenJLPT](https://github.com/evanclan/OpenJLPT)** as the primary
external grammar-reference candidate. It reports 526 entries with stable IDs,
patterns, formation descriptions, examples, and CC BY-SA 4.0 licensing with
attribution; its descriptions and examples require independent quality checks.
Formation descriptions are human-readable, not executable matching rules, and
JLPT-focused coverage will miss many colloquial or literary constructions.
Pin a release or commit and hash the exact files if adopted. Inspect the
specific pilot entries, schema, notice and upstream attribution before copying
or distributing anything; satisfy applicable attribution and share-alike terms.

Use **[Japanese Language Data](https://github.com/jkindrix/japanese-language-data)**
as a secondary source to compare coverage and explanations. Its repository
reports 595 structured grammar points under CC BY-SA 4.0, but also explicitly
marks all of them draft and not yet reviewed by native-speaker linguists. Treat
them as leads, not verified gold. Check individual-source provenance and license
before reuse. Neither dataset's JLPT classification is a filter or a complete
inventory of Japanese grammar. Do not scrape sources with unclear reuse rights.

Maintain a small **project-owned, versioned pattern catalog** for the pilot:
stable construction ID, display form, brief sourced explanation or reference,
component constraints, permitted variants, ambiguity notes and source/version
references. Manually encode and review each matching rule; do not automatically
turn every external formation string into a rule. Keep external text separate
from project-authored rules unless its reuse terms and attribution are handled.
Record disagreements and missing coverage rather than asserting an unsupported
meaning.

Evaluate **[KWJA](https://github.com/ku-nlp/kwja)** separately as an optional
source of morphology, dependencies and named-entity evidence. It is an analyzer,
not a grammar dictionary. It has not been tested on this corpus; check model and
dependency licenses, installation, CPU runtime, memory, versions, and source-span
alignment before adoption. Preserve the original text if it proposes a
normalization. Its outputs must not override source evidence or be treated as
automatically correct.

## Where the pipeline changes

Current executable order is extraction → normalization → Sudachi raw tokens →
greedy JMdict merge → spelling-keyed `BuildWordStatsStep` → frequency filter →
definitions → scoring → examples → CSV → optional Anki. The merger can hide
useful token boundaries; `TokenComponent` retains component evidence but no
independent source spans. `SegmentedSentence` maps one surface and span per
lemma, so repeated occurrences can overwrite one another. Raw source is not
currently mapped through normalization. Standalone proper nouns are filtered.

Proposed flow:

```text
source sentence + normalization mapping
   → raw Sudachi tokens and aligned occurrences ─┐
   → existing dictionary merges ─────────────────┤
   → optional KWJA / other analysis ─────────────┤
   → grammar proposal matcher ◄──────────────────┘
   → occurrence-level alternatives and evidence
   → lexical and grammar candidate identities → review/export
```

1. **Source and occurrence envelope.** Extend extraction and normalization to
   retain source identity, original and normalized sentence, and an explicit
   offset mapping. Handle deletions, expansions and many-to-one normalization;
   if an exact mapping cannot be established, record an unmappable span rather
   than fabricate one. Give every token and grammar occurrence a stable ID and
   preserve repeated occurrences and component links. Define ID stability under
   changed source content and normalization versions. Keep adapters for current
   consumers until migrated.
2. **Parallel evidence.** Capture raw tokens before `MergeTokensStep`; retain
   merged proposals and their constituent occurrence IDs. A new shadow grammar
   stage receives both views, sentence context and catalog; it emits overlapping
   proposals without mutating the streams consumed by `BuildWordStatsStep`.
   If a required component or source span is unavailable, emit an unresolved
   result rather than inventing a match.
3. **Grammar proposal contract.** Record construction ID, normalized and raw
   spans when available, exact source surface, component IDs, rule/version,
   matched constraints, source references, alternatives, and a status such as
   supported, ambiguous or unresolved. These are evidence labels, not calibrated
   probabilities. Keep lexical candidates and valid larger expressions even
   when a grammar proposal overlaps them.
4. **Aggregation and definitions.** After validating shadow results, introduce
   typed identities such as lexical entry and grammar construction rather than
   keying all uses by spelling. Count each supported occurrence once per
   identity, keep unassigned occurrences visible, and draw examples only from
   compatible occurrences. Adapt frequency filtering, `WordStats`, definition
   lookup, scoring and `AttachSentencesStep`; do not silently merge grammar and
   lexical senses or discard standalone proper nouns. Design the proper-noun
   eligibility change with focused recall tests as a separate explicit slice.
5. **Outputs and compatibility.** First write a versioned shadow JSONL artifact
   with sentence IDs, proposals, evidence and unresolved alternatives; keep
   current CSV/Anki output unchanged. Later add an opt-in audit/export profile
   for separate identities. Specify note keys, existing-note migration,
   duplicates, user edits and prune behavior before any Anki integration; test
   plans and migration with a fake Anki service. Never use real Anki mutations
   during tests.
6. **Reproducibility and cost.** Add a disabled-by-default grammar configuration
   and record catalog, rule, dictionary, analyzer/model and schema versions and
   hashes in the run archive. Cache only after measuring; keys must include raw
   inputs, normalization, tokenization, catalog, rules and optional model
   identity. Grammar diagnostics contain source text and must remain local by
   default. Account for archive size, CPU time and memory.

## Implementation order and checks

1. Inspect pilot examples and the two source datasets; select and document
   actual pilot entries, reuse rights, and whether their formation descriptions
   cover the cases. Pin exact versions before importing data. Create a small
   catalog and positive, negative, ambiguous and overlapping fixtures.
2. Add occurrence IDs and raw/normalized provenance with repeated-occurrence
   and normalization alignment tests. Preserve current output; measure any
   memory and runtime cost on a bounded sample.
3. Implement the opt-in shadow matcher and JSONL diagnostics for the three
   pilot families. Test split and merged tokenizations, contractions, rejected
   or ambiguous interpretations, lexical controls, failed alignment and cache
   equivalence where applicable.
4. Run a bounded, Anki-disabled sample and review proposals against original
   sentences. Use the existing candidate scorer where its exact span/target
   contract fits; extend or supplement it for grammar identities, alternatives
   and supported examples. Include raw-source material missed by the pipeline.
   Keep a source-separated, duplicate-audited holdout for a later independent
   check; report uncertainty and abstentions, not only the review queue size.
5. Trial KWJA independently on the same examples and compare incremental
   benefit, failure cases and resource cost. Do not make it a required dependency
   unless the comparison justifies it. Decide whether contextual selection is
   necessary for ambiguous cases; evaluate any model with constrained source and
   dictionary evidence and an explicit abstention option.
6. Only after the shadow pilot demonstrates useful results, design and implement
   typed candidate aggregation and an opt-in export. Add regressions for both
   corrected false positives and useful retained forms. Compare full-corpus
   output with Anki disabled, then separately review note migration and safety.

Run `python scripts/run_tests.py` for pipeline changes and report any
missing integration dependencies. Success for the pilot means demonstrably useful
source-grounded proposals, preserved components and ambiguity, no unexplained
loss of useful lexical candidates, and a reviewable comparison—not universal
sentence understanding or a predetermined improvement percentage.
