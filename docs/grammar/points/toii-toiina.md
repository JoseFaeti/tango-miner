# 〜といい / 〜といいな

Status: [~]

## Meaning

Hope that something happens; it would be good if.

## Supported behavior and limitations

A source-adjacent finite verb, adjective, or auxiliary followed by the
particle `と` (conjunctive or case-particle analysis) and `いい`, `よい`, or
`良い` produces a match.
An adjacent sentence-final `な` is included. The preceding predicate remains
available as lexical evidence. Other following particles and polite endings
are outside the match. The rule does not distinguish a wish from a literal
conditional evaluation; corpus coverage and dictionary-expression precedence
remain unmeasured. Other inflections and tokenizations are not covered.

## Implementation and evidence

- Rule: [toii_toiina/index.py](../../../src/grammar_rules/rules/toii_toiina/index.py), identity `toii_toiina`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/toii_toiina/test_index.py), including Sudachi forms, lexical controls, reservation and card example.
