# 〜ておく

Status: [~]

## Meaning and tracked scope

preparation or leaving a state.

  - Covered: split て/で + おく and auxiliary とく/どく, including selected
    plain, past, negative, polite, て-form, conditional and volitional forms.
  - Remaining: ambiguous analyses such as 読んどいた, merged tokens, negative
    antecedents, 置く spelling and other inflections. Evaluate lexical expression precedence,
    contextual meanings and independent precision and recall. Longer JMdict
    expressions beginning at おく take precedence; effects on output frequencies
    need a same-input corpus comparison.

## Implementation

- Rule: [src/grammar_rules/rules/te_oku/index.py](../../../src/grammar_rules/rules/te_oku/index.py)
- Tests: [tests/test_grammar_te_oku.py](../../../src/grammar_rules/rules/te_oku/test_index.py)
- Registry identity: `te_oku`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ておく` card matches a continuative verb or auxiliary followed by
split て/で + non-independent おく, or a Sudachi とく/どく auxiliary.
Supported complete forms include present, past, negative, polite, て-form,
conditional and volitional forms, including たら as a complete token. The
conditional card can also match the same たら. Ambiguous analyses such as
読んどいた, negative antecedents such as 忘れないでおいて and the 置く
spelling remain lexical. The meaning can be preparation or leaving a resulting
state; tokenization cannot establish which.
A raw replay of the first 20,000 archived sentences found 123 matches, before
expression precedence and example eligibility; this does not measure precision
or recall. In the archived ほっとく example, an exact JMdict abbreviated verb
now takes precedence over grammar, preserving the complete lexical candidate
and its ほっ and とく component evidence. Ordinary 買っとく remains grammar.
Independent おく and 置く remain eligible.
When a longer JMdict expression starts at おく and extends beyond the
construction, as in おけばいい or おけばよかった, it takes precedence over
this grammar match. The attached verb remains eligible; in 従っておく, the
merger leaves 従う as a verb rather than selecting the adverb 従って. These
changes are validated on selected examples and have not been compared in a
full corpus. The interpretation and wider frequency effects need further evaluation.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
