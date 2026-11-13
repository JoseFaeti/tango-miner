# 〜上に

Status: [~]

## Meaning

In addition; moreover.

## Implementation

- Rule: [ueni](../../../src/grammar_rules/rules/ueni/index.py), identity `ueni`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/ueni/test_index.py).
- Predicate-attached 上に is recognized after verbs, adjectives and auxiliaries
  in terminal or attributive form, with a following clause beginning. Sudachi
  full-dictionary tests cover past verbs, adjectives and copular な; the span
  reserves 上 and に while retaining the attached predicate.
- Noun + の is excluded because it also expresses location, so additive noun
  forms are missed. Predicate-attached spatial readings such as a physically
  high surface are also possible; token context alone cannot distinguish them.
  Other tokenizations and whether the following clause is actually additive
  remain unverified. The clause check only requires a subsequent non-symbol token.
