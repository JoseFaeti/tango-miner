# 〜ものと思われる

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

It is considered or believed that; a formal inference.

## Tracked scope

Matches an adjacent attributive verb, adjective or auxiliary followed by
`ものと思われる`, `ものと思われた`, `ものと思われます`, or
`ものと思われました`. The span starts at `もの`. Full-dictionary tests cover
these forms, spacing and lexical controls. A literal thing reading such as
`彼が買ったものと思われる` can also match. Active matches reserve the
matched tokens while leaving the preceding predicate available. Dictionary
expression precedence can suppress grammar occurrences. Negative and other
inflections, alternate spellings and merged analyses remain unsupported;
corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [monotoomowareru](../../../src/grammar_rules/rules/monotoomowareru/)
- Registry identity: `monotoomowareru`; stable identity: `〜ものと思われる`.
