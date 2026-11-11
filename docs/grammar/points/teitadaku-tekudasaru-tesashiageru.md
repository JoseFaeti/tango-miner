# 〜ていただく / 〜てくださる / 〜て差し上げる

Status: [~]

## Meaning

Respectful or humble benefactive: receive the benefit of someone's action,
someone respectfully does something for the beneficiary, or humbly do something
for someone else.

## Implementation and limitations

The `te_polite_benefactive` rule recognizes a continuative verb or auxiliary,
split て/で, and いただく, くださる or 差し上げる in selected plain,
past, polite, negative, connective, conditional and volitional forms. Sudachi
examples cover kana and kanji spellings, including さしあげる. 差し上げる is
classified as an independent verb by Sudachi; the adjacent verb and connective
are required to distinguish this construction from ordinary giving. The
construction is reserved while the attached predicate remains eligible for
lexical collection, subject to shared dictionary expression precedence.

Focused analyzer tests cover forms, standalone giving verbs, てください and
ていただけませんか, plus card identity and reservation. The request
てください and ていただけませんか have their own cards; potential
いただける requests are outside this rule. Other inflections, spellings, negative antecedents and merged tokenizations
remain to be investigated. Morphology alone cannot establish the beneficiary or
resolve all contextual interpretations. Corpus precision, recall, frequency and
expression precedence effects remain unevaluated.

See [rule](../../../src/grammar_rules/rules/te_polite_benefactive/index.py),
[tests](../../../src/grammar_rules/rules/te_polite_benefactive/test_index.py) and
[stage and precedence](../stage-and-precedence.md).
