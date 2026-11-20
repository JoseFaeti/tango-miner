# 〜とはいえない

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Cannot say that; a proposition cannot necessarily be asserted.

## Implementation

The rule matches adjacent と + は + potential 言える / いえる with negative
ない, past and selected continuative and conditional inflections, or polite
ません (including ませんでした). The span leaves the preceding proposition
available. Sudachi full-dictionary tests cover kanji and kana, inflection,
concessive とはいえ and unrelated 言えない controls, and card reservation.
The construction can also literally mean inability to say something; token
matching cannot resolve that ambiguity. Literary ぬ and other variants remain
unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/tohaienai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tohaienai/test_index.py)
- Registry identity: `tohaienai`; stable expression: `〜とはいえない`.
