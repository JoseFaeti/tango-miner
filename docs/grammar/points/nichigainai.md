# 〜に違いない

Status: [~]

## Meaning

Strong inference: must be; surely.

## Implementation

Recognizes source-adjacent に違いない and にちがいない after a nominal,
adjectival noun, or finite predicate, including なかった, なくて and なければ.
A following です is outside the span. The attached word remains lexical;
reservation is subject to dictionary expression precedence. Sudachi full
dictionary analyses and reservation are covered by focused tests. Literal
comparisons can be ambiguous in context, and merged analyses and other
inflections are not yet covered. Independent precision and recall evaluation
remain outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/nichigainai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/nichigainai/test_index.py)
- Identity: `nichigainai` / `〜に違いない`
