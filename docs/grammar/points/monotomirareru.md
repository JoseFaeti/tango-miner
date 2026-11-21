# 〜ものと見られる

Status: [~]

## Meaning

It is considered or believed that; a formal inference.

## Tracked scope

Matches an adjacent attributive verb, adjective or auxiliary followed by
`ものと見られる`, `ものと見られた`, `ものと見られます`, or
`ものと見られました`. The span starts at `もの`. Full-dictionary tests cover
these forms, spacing and lexical controls. A literal thing reading such as
`彼が買ったものと見られる` can also match. Active matches reserve the
matched tokens while leaving the preceding predicate available. Dictionary
expression precedence can suppress grammar occurrences. Negative and other
inflections, alternate spellings and merged analyses remain unsupported;
corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [monotomirareru](../../../src/grammar_rules/rules/monotomirareru/)
- Registry identity: `monotomirareru`; stable identity: `〜ものと見られる`.
