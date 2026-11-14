# 〜に相違ない

Status: [~]

## Meaning

Formal strong inference: must certainly be; without doubt.

## Implementation

Recognizes source-adjacent に相違ない after a nominal, adjectival noun, or
finite predicate, including なかった, なくて and なければ. A following です
is outside the span. The attached word remains lexical; reservation is subject
to dictionary expression precedence. Focused tests cover Sudachi full dictionary
analyses, lexical controls, reservation and card identity. Literal statements
about differences can be ambiguous in context. Kana spelling, merged analyses,
other inflections, and independent precision and recall evaluation remain open.

- Rule: [index.py](../../../src/grammar_rules/rules/nisouinai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/nisouinai/test_index.py)
- Identity: `nisouinai` / `〜に相違ない`
