# 〜とは限らない

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Not necessarily; a proposition is not always true.

## Implementation

The rule matches adjacent と + は + 限る / かぎる with negative ない,
past and selected continuative and conditional inflections, or polite ません
(including ませんでした). The preceding proposition remains available.
Sudachi full-dictionary tests cover kanji and kana, inflection, unrelated
限らない and とは限る controls, and card reservation. A literal restriction
reading can be ambiguous in context. Literary ぬ and other variants remain
unverified; 〜とも限らない is tracked separately.

- Rule: [index.py](../../../src/grammar_rules/rules/tohakagiranai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tohakagiranai/test_index.py)
- Registry identity: `tohakagiranai`; stable expression: `〜とは限らない`.
