# 〜ないわけにはいかない

Status: [~]

## Meaning

Cannot avoid doing something; circumstances or obligation make it necessary.

## Implementation

Recognizes a source-adjacent verbal negative ない followed by わけ / 訳,
に or には, and いかない / 行かない in plain present or past and polite
negative present or past forms. The attached verb remains lexical; dictionary
expression precedence can affect reservation. Focused tests cover Sudachi full
dictionary analyses, positive-predicate and adjective controls, reservation,
and card identity alongside 〜わけにはいかない and 〜ない.

Other inflections, merged tokens, and contextual ambiguity remain; independent
precision and recall evaluation is outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/naiwakenihaikanai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/naiwakenihaikanai/test_index.py)
- Identity: `naiwakenihaikanai` / `〜ないわけにはいかない`
