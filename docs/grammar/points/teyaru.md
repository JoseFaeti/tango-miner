# 〜てやる

Status: [~]

## Meaning

Doing something for someone, or expressing a determined or defiant intention
to do something. Context determines the interpretation and the social tone.

## Implementation and limitations

The `te_yaru` rule matches a continuative verb or auxiliary followed by split
て/で and non-independent やる in selected plain, past, polite, negative,
connective, conditional and volitional forms. Focused Sudachi tests cover these
forms and standalone やる, a noun followed by で, and a separated auxiliary.
The construction is reserved while the attached verb remains eligible for
lexical collection, subject to shared dictionary expression precedence.

Morphology cannot distinguish benefactive and determined uses or their tone.
Other inflections, contractions, kanji spellings, negative antecedents and
merged tokenizations remain to be investigated. Expression precedence,
frequency effects and independent precision and recall remain unevaluated.

See [rule](../../../src/grammar_rules/rules/te_yaru/index.py),
[tests](../../../src/grammar_rules/rules/te_yaru/test_index.py) and
[stage and precedence](../stage-and-precedence.md).
