# 〜ばよかった / 〜たらよかった

Status: [~]

## Meaning

Regret or a wish about a past alternative: “should have” or “wish I had.”

## Supported behavior and limitations

Recognizes source-adjacent conditional `ば` or `たら`/`だら` followed by
`よかった` or `良かった`, split into an adjective and past auxiliary. It
uses the existing conditional rules' predicate and inflection checks, including
supported negative and adjective predicates. The span begins at the conditional,
leaving the preceding predicate as lexical evidence; conditional cards can also
match. Dictionary-expression precedence may suppress a grammar proposal.

The matcher cannot distinguish regret from a literal conditional evaluation or
other context-dependent readings. Other tokenizations and contractions are not
covered; corpus coverage and independent precision and recall remain unevaluated.

## Implementation and evidence

- Rule: [bayokatta_tarayokatta/index.py](../../../src/grammar_rules/rules/bayokatta_tarayokatta/index.py), identity `bayokatta_tarayokatta`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/bayokatta_tarayokatta/test_index.py), including Sudachi forms, negative predicates, lexical controls, reservation and overlapping conditionals.
