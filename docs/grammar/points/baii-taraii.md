# 〜ばいい / 〜たらいい

Status: [~]

## Meaning

Suggestion, wish, or sufficiency: it would be good if, one should, or one
only needs to do something. Context determines the reading.

## Supported behavior and limitations

The rule recognizes source-adjacent conditional `ば` or `たら`/`だら`
followed by `いい`, `よい`, or `良い`. The conditional must satisfy the
existing predicate and inflection checks for the individual conditional rules;
negative and adjective predicates are included when those checks succeed. The
matched span starts at the conditional, preserving the preceding predicate as
component evidence. The conditional cards may also match the same sentence;
dictionary-expression precedence can suppress an overlapping grammar proposal.

Past `よかった` belongs to the [separate regret point](bayokatsuta-tarayokatsuta.md). Other inflections,
contractions, and tokenizations are not covered. The matcher is morphological:
it does not distinguish advice, wishes, and literal conditional evaluations.
Corpus coverage and expression precedence for this point remain unmeasured.

## Implementation and evidence

- Rule: [baii_taraii/index.py](../../../src/grammar_rules/rules/baii_taraii/index.py), identity `baii_taraii`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/baii_taraii/test_index.py), including Sudachi forms, negative predicates, lexical controls, reservation and card example.
- Shared behavior: [stage and precedence](../stage-and-precedence.md), [identity and review](../identity-and-review.md).
