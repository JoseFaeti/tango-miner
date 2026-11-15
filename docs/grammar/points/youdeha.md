# 〜ようでは

Status: [~]

## Meaning

If the situation is such, often implying an undesirable consequence.

## Tracked scope

The rule matches predicate-attached `ようでは` and colloquial `ようじゃ` with
a following clause. It covers verbs, adjectives and auxiliary predicates,
including na-adjective forms, and preserves the preceding predicate as lexical
evidence. Directly attached `ない` and `ある` are excluded to avoid ordinary
negative `ようではない` and `ようではありません` readings. Tests use Sudachi
full-dictionary tokenization and cover these negatives, lexical `用`, spacing,
incomplete sentences and card examples.

The matcher cannot establish whether the consequence is undesirable or whether
the following tokens form a complete clause. Its negative guard can miss a
conditional followed directly by a negative consequence; noun-attached forms,
other tokenizations and corpus precision and recall remain unassessed.

## Implementation

- Rule and tests: [youdeha](../../../src/grammar_rules/rules/youdeha/)
- Registry identity: `youdeha`; stable identity: `〜ようでは`.
