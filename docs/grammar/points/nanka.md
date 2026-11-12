# 〜なんか

Status: [~]

## Meaning

Examples, emphasis or disparagement.

## Implementation

The `nanka` rule recognizes the source-aligned Sudachi adverbial particle
なんか attached to a preceding token. It reserves only the particle, retaining
surrounding lexical evidence, and uses the stable identity `〜なんか`. The
combined `など / なんか / なんて` rule no longer records these occurrences.
The longer `〜たりなんかして` rule can also record an overlapping occurrence;
matching rules are independently counted.

Tests cover noun and clause attachment, indefinite and discourse controls,
whitespace, reservation, identity and the combined rule. The rule does not
distinguish examples from emphasis or disparagement, or recognize detached,
sentence-initial or other tokenizations.

- [Rule](../../../src/grammar_rules/rules/nanka/index.py)
- [Tests](../../../src/grammar_rules/rules/nanka/test_index.py)
