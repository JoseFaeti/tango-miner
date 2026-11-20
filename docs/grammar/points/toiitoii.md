# 〜といい〜といい

- [Meaning](#meaning)
- [Supported behavior and limitations](#supported-behavior-and-limitations)
- [Implementation and evidence](#implementation-and-evidence)

Status: [~]

## Meaning

Both examples; whether one aspect or another, the same evaluation applies.

## Supported behavior and limitations

Two noun- or pronoun-attached `といい` or `と言い` markers in the same
sentence produce a match on the first marker, leaving the examples available
as vocabulary. Commas between examples and repeated pairs are supported.
The spelling `いい` is analyzed as an adjective by Sudachi, so ordinary
phrases can be ambiguous; the rule cannot establish the intended meaning.
Other tokenizations and non-noun examples are not covered. Corpus coverage and
dictionary-expression precedence remain unmeasured.

## Implementation and evidence

- Rule: [toiitoii/index.py](../../../src/grammar_rules/rules/toiitoii/index.py), identity `toiitoii`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/toiitoii/test_index.py), including Sudachi forms, controls, reservation and card identity.
