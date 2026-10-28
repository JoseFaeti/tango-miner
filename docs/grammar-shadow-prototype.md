# Grammar shadow prototype

The opt-in pipeline mode writes `grammar-shadow.jsonl` in the run archive,
after raw tokenization and before dictionary merging:

    python -X utf8 tango_miner.py --input path/to/corpus --output path/to/output.csv --grammar-shadow

The normal configured Anki behavior still applies; disable Anki explicitly in
the configuration before using this for a safe corpus check. The shadow stage
does not change token streams, candidates, CSVs, or Anki plans. Output contains
normalized sentences, raw token evidence and source paths. Original text and
source offsets are not yet carried through normalization, so raw spans are null.
Sentence IDs in this pipeline mode depend on source path, sentence index and
normalized text, and can change when input ordering changes.

An offline mode reads samples and analyzer JSONL from the existing analyzer comparison:

    python -X utf8 -m src.GrammarShadowCLI --sample checks/raw.jsonl --analysis checks/sudachi.jsonl --output checks/grammar-shadow.jsonl

Create the output directory first; the output is created exclusively. Rows contain source text, aligned analyzer tokens and tentative proposals. Despite the normalized_sentence field name, this CLI passes sample text unchanged and records identity raw spans. These offsets apply to the raw sample line, not a pipeline-normalized sentence. Samples and evidence may be private.

The initial catalog detects literal んだ, ている, でいる, てる, でる and ものの. A narrow token-based rule also proposes inflected ている and contracted forms such as でいた and てた when the observed tokens meet its constraints. Every match is ambiguous, including fragments and unrelated lexical uses. This is a discovery baseline, not a validated grammar classifier. It can miss inflected and other variants. The evidence records whether boundaries match and whether the token analysis contains a nominalizer and copula, connective and いる, or particular ものの analyses. This evidence is not confirmation. Alignment failures produce no proposals. In offline mode sentence and proposal IDs depend on the sample ID and text, not sample order; changing either changes the IDs. The rule file hash accompanies successful rows, and offline rows also include analyzer metadata.

A targeted six-line sample yielded two proposals; a 69-line raw sample yielded six. Neither result measures correctness or recall. No external grammar data has been imported or pinned. OpenJLPT is a potential reference; inspect and pin exact entries, verify quality and comply with its CC BY-SA 4.0 notice before reuse. The prototype records general reference links only.

In the 69-line Sudachi sample, version 2 reports four connective-plus-いる
analyses, one んだ with matching token boundaries, and one ている-family
match inside a token; no alignment errors were reported. These are analyzer
observations, not independently reviewed correctness judgments.

A later 25-sentence synthetic pilot ran with Anki disabled and a workspace-local
cache. Baseline and shadow runs produced byte-identical main, dropped and
sentence CSV files; the shadow artifact recorded 25 tentative proposals and no
alignment errors. These repeated, selected examples check integration and do
not establish general accuracy or recall. Real `死んだ` tokenization also
produced a tentative `んだ` match; it must remain for review, not become an
explanatory definition automatically. Token analyses identified past and polite
`〜ている` and one contracted past form in a small hand-selected probe.

OpenJLPT's `te-iru`, `mono-no`, and `n-desu-no-desu` entries are relevant
references for the pilot forms. Their human-readable formations are not
executable rules; no entry text was imported. The links in output follow the
upstream main branch and are not pinned data. Before importing explanations or
using them for automatic definitions, pin and review the source and satisfy its
license and attribution requirements.

In the full-corpus shadow run dated 2026-10-28, 521,821 normalized sentences
produced 76,854 tentative proposals with no alignment failures. The JSONL was
approximately 1.19 GB, and the shadow stage took 34.6 seconds. This run used
Anki preview mode; it did not apply its proposed note changes. Inspection
found incomplete polite-form surfaces such as `ていまし` and `てませ`.
Version 4 extends only adjacent, token-supported endings, for example to
`ていました` or `ていません`; unresolved tokenizations remain tentative.
Replaying 810 occurrences that had these partial surfaces from the archive
showed the majority extending to longer surfaces. This is a targeted diagnostic,
not an independent accuracy or recall assessment; no corpus rerun has yet
measured the revised rules. Raw-source spans and a compact archive format remain
open work.

Remaining work includes original-to-normalized source mapping, broader
component-aware rules, merged-token evidence and independent review. Standalone
proper nouns remain filtered by the existing pipeline. This experimental layer
does not affect CSV or Anki.

## First opt-in grammar card

Pass `--grammar` to run the shared grammar stage; the older `--grammar-te-iru`
option is an alias. The stage checks all registered rules, currently just one,
and adds a separate `〜ている (grammar)` candidate to
the normal CSV and, when Anki is enabled, to its usual plan. Only uninflected
source occurrences with adjacent Sudachi continuative verb/auxiliary + connective て/で +
non-independent `いる` evidence are counted. This covers a limited subset of
the grammar; contracted, past, polite and other variants are not counted. The
example must also pass the existing sentence-validity check. No card is emitted
without an eligible example. Its frequency counts only the supported subset,
and index and score are zero placeholders rather than corpus ranking. It is
added after ordinary lexical scoring and filtering; the minimum-frequency
threshold does not apply to this one grammar card. The lexical candidates and
their examples are unchanged. This is a narrow evidence rule, not a guarantee
that every occurrence has a single unambiguous aspectual meaning.

A replay of the first 15,000 sentences in a full-corpus shadow archive found 25
matching occurrences after the continuative-form restriction. A prior version
also matched a name fragment that Sudachi analyzed as a non-continuative verb;
the restriction excludes that observed case. This bounded, ordered probe and
the three inspected examples are not a representative evaluation and cannot
establish precision or recall. An Anki-disabled small pipeline run produced the
new card and a source-linked, highlighted example.

The distinct Expression is the Anki identity. A new card participates in the
existing preview and confirmation flow. If a note with that exact expression
already exists in the configured deck/model, it is preserved rather than
updated; this protects user edits but means its frequency and examples do not
refresh automatically. The plan lists it under `retained_existing_grammar`.
The identity is also protected from pruning in later runs where the flag is
absent or no occurrence is found. The flag does not enable Anki or disable
other planned updates. Review the complete Anki plan before applying it.

The agreed naming convention uses `〜` to indicate an attachment point, where
appropriate, and `(grammar)` as a distinctive suffix, for example
`〜ている (grammar)`. The attachment marker alone does not guarantee uniqueness
against vocabulary expressions. Treat the full expression as a stable identity;
changing it later requires an explicit migration plan for existing notes.
The shared grammar stage collects all registered rules in one pass; the output
writer adds its cards after lexical scoring. When registering another rule,
register its identity in the Anki protection set as well; startup validates
that both registrations match.

For a bounded proposal-review sample from an existing shadow archive:

    python -X utf8 -m src.GrammarReview --input runs/example-run/grammar-shadow.jsonl --output checks/grammar-review.jsonl --per-stratum 5

The sampler streams the archive and deterministically selects up to the limit
per construction and evidence combination. Rows include stratum population,
source text, tokens, proposal, and empty human-review fields. They are
proposal-selected and stratified, not a representative sentence sample; they
cannot measure missed grammar or corpus-wide accuracy. Their contents may be
private. Do not use the same examples for both rule design and independent
evaluation.
