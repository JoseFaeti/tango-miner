# 〜てあげる / 〜てくれる / 〜てもらう

Status: [~]

## Meaning

Doing something for someone, receiving the benefit of an action, or having
someone do something for the speaker or another beneficiary.

## Implementation and limitations

The `te_benefactive` rule recognizes a continuative verb or auxiliary followed
by split て/で and non-independent あげる, くれる or もらう in selected
plain, past, polite and negative forms. The construction is reserved while the
attached predicate remains eligible for lexical collection, subject to shared
dictionary expression precedence. The imperative てくれ belongs to the
separate request card. Focused Sudachi tests cover these forms, ordinary giving
verbs, requests, card examples and reservation.

Other inflections, contractions, kanji spellings, negative antecedents and
merged tokenizations remain to be investigated. Morphology alone cannot resolve
the beneficiary or every contextual interpretation. Expression precedence,
frequency effects and independent precision and recall remain unevaluated.

See [rule](../../../src/grammar_rules/rules/te_benefactive.py),
[tests](../../../tests/test_grammar_te_benefactive.py) and
[stage and precedence](../stage-and-precedence.md).
