# 〜ようと〜まいと

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Whether or not.

## Tracked scope

Recognizes adjacent paired forms with the same verb, including Sudachi's
analyses of 降ろうと降るまいと, 食べようと食べまいと, しようとしまいと
and 来ようと来まいと. The full pair is reserved as one grammar span. Tests
cover these forms, mismatched verbs, incomplete pairs, spacing, mixed particles,
and the separate 〜ようが〜まいが construction. A following と can also
introduce a quotation, so a sequence such as 行こうと行くまいと思う is
ambiguous and currently matches. Other tokenizations, punctuation, verb
inflections, and independent corpus precision and recall remain unassessed.
Dictionary expression precedence can suppress a match.

## Implementation

- Rule and tests: [youtomaito](../../../src/grammar_rules/rules/youtomaito/)
- Registry identity: `youtomaito`; stable identity: `〜ようと〜まいと`.
