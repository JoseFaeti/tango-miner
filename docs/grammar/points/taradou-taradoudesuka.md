# 〜たらどう / 〜たらどうですか

Status: [~]

## Meaning

Suggestion: why not do something; how about doing it?

## Supported behavior and limitations

Source-adjacent conditional `たら` or `だら` followed by sentence-final
`どう` or `どうですか` is recognized using the existing conditional predicate
and inflection checks. The span starts at the conditional, retaining the
preceding predicate as component evidence. The conditional card can overlap.
The morphological match cannot distinguish a suggestion from a literal question
about what would happen. Other endings such as `どうかな` and `どうだろう`,
and corpus coverage and dictionary-expression precedence, remain unverified.

## Implementation and evidence

- Rule: [taradou/index.py](../../../src/grammar_rules/rules/taradou/index.py), identity `taradou`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/taradou/test_index.py), including Sudachi tokenization, controls, reservation and card example.
