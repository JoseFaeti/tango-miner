# 〜に決まっている

Status: [~]

## Meaning

Strong conviction: surely; must be.

## Implementation

Recognizes source-adjacent に決まっている and にきまっている after a
nominal, adjectival noun, or finite predicate, including supported いる
inflections such as いた, います and いない. The attached word remains
lexical; reservation is subject to dictionary expression precedence. Focused
tests cover Sudachi full dictionary analyses, lexical controls and reservation.
Literal decisions such as a person being chosen can have the same form and
remain ambiguous. Contracted に決まってる is tracked separately; merged
analyses and independent precision and recall evaluation remain outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/nikimatsuteiru/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/nikimatsuteiru/test_index.py)
- Identity: `nikimatsuteiru` / `〜に決まっている`
