# 〜はずがない

Status: [~]

## Meaning

Strong inference that something cannot be the case; there is no way that.

## Implementation

Recognizes source-adjacent split はずがない after finite verbs, adjectives,
and auxiliaries, including なかった, なくて and なければ. A following です
is outside the span. The attached predicate remains lexical; reservation is
subject to dictionary expression precedence. Focused tests cover Sudachi full
dictionary analyses, lexical controls, reservation and card identity.

Noun + の is excluded because literal noun readings can share that analysis.
Particle omission (はずない), other variants and merged analyses are not
covered. Contextual ambiguity remains, and independent precision and recall
evaluation is outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/hazuganai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/hazuganai/test_index.py)
- Identity: `hazuganai` / `〜はずがない`
