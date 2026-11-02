# 6. Preserve proper-noun evidence through token merging

[Contents](../recommended-improvements-in-priority-order.md)


Raw token analysis can identify proper nouns, but merging may replace that
information with a generic dictionary entry. Preserve this evidence as context
for target and definition selection. Names and game-specific terms remain in
scope because they occur in the reading material; do not exclude them merely
because they are proper nouns.

**Current limitation (2026-10-27):** merge components retain raw proper-noun
POS, but the existing token filter excludes standalone `名詞` / `固有名詞`
tokens before candidate aggregation. A merged candidate can retain that
evidence if it passes the existing filter. Provenance does not reverse the
filter or make names eligible; aligning that behavior with the recall-first
policy remains future work requiring focused regression tests.

