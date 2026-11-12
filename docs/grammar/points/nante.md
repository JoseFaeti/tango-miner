# 〜なんて

Status: [~]

## Meaning

Surprise, example or disparagement.

## Implementation

The `nante` rule recognizes source-aligned Sudachi adverbial particle なんて
attached to a preceding token, including nouns and clauses. It reserves only
the particle and retains the surrounding lexical evidence. Its stable identity
is `〜なんて`. The previous combined `など / なんか / なんて` card no longer
receives these occurrences, preventing duplicate grammar cards.

Tests cover attachment, interrogative and exclamatory controls, whitespace,
reservation, identity and the combined rule. The rule does not distinguish
surprise, examples and disparagement, or match other tokenizations, detached
uses, or sentence-initial exclamatory なんて.

- [Rule](../../../src/grammar_rules/rules/nante/index.py)
- [Tests](../../../src/grammar_rules/rules/nante/test_index.py)
