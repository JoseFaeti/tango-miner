# 〜ことにしている

Status: [~]

## Meaning

Habitual decision or a rule one has adopted. The same form can express an
ongoing decision whose habitual meaning depends on context.

## Implementation

- Rule: [koto_ni_shiteiru](../../../src/grammar_rules/rules/koto_ni_shiteiru/index.py)
- Tests: [focused regressions](../../../src/grammar_rules/rules/koto_ni_shiteiru/test_index.py)
- Registry identity: `koto_ni_shiteiru`.

The matcher supports a finite verb or supported verbal negative followed by
split こと + に + し and the forms recognized by the 〜ている rule, including
polite, past and contracted forms. The predicate remains available as lexical
evidence; the construction is reserved subject to dictionary expression
precedence. Sudachi full-dictionary examples and noun, spacing, and other-verb
controls are tested. Other tokenizations and contextual distinctions remain
unevaluated; precision and recall have not been measured.
