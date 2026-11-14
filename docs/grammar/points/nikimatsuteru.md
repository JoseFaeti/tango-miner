# 〜に決まってる

Status: [~]

## Meaning

Colloquial certainty.

## Implementation

Recognizes source-adjacent に決まってる and にきまってる after a nominal,
adjectival noun, or finite predicate, including supported contracted てる
inflections such as てた. The attached word remains lexical; reservation is
subject to dictionary expression precedence. The general 〜ている rule can
also produce a card for the same occurrence. Literal decisions such as a person
being chosen are ambiguous. Merged analyses and independent precision and
recall evaluation remain outstanding.

Focused tests cover Sudachi full dictionary analyses, lexical controls,
reservation, and identity.

- Rule: [index.py](../../../src/grammar_rules/rules/nikimatsuteru/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/nikimatsuteru/test_index.py)
- Identity: `nikimatsuteru` / `〜に決まってる`
