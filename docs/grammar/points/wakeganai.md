# 〜わけがない

Status: [~]

## Meaning

Strong inference that something is impossible; there is no way that.

## Implementation

Recognizes source-adjacent split わけがない after finite verbs, adjectives
and auxiliaries, including なかった, なくて and なければ. A following です
is outside the span. The attached predicate remains lexical; reservation is
subject to dictionary expression precedence. Focused tests cover Sudachi full
dictionary analyses, lexical controls, reservation and card identity.

Noun + の is excluded because literal reason readings can share that analysis.
Particle omission, other variants and merged analyses are not covered.
Contextual ambiguity remains, and independent precision and recall evaluation
is outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/wakeganai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/wakeganai/test_index.py)
- Identity: `wakeganai` / `〜わけがない`
