# より

Status: [~]

## Meaning and tracked scope

Comparison: “than; compared with.” The rule covers a noun or pronoun followed
directly by the particle and an adjective, such as 昨日より寒い. Other comparisons,
including intervening subjects and verbal predicates, remain unsupported.

## Implementation

- Rule: [index.py](../../../src/grammar_rules/rules/yori/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/yori/test_index.py)
- Registry identity: `yori`; stable expression: `〜より`, distinct from lexical `より`.

The matcher requires adjacent source-aligned tokens and Sudachi's case-particle
analysis. It reserves only より, retaining the compared noun and adjective as
lexical evidence. It does not attempt to infer comparison across a clause;
source, starting-point, adverbial より and lexical forms are outside this narrow
pattern. An adjective immediately following a starting-point use can still be
ambiguous. Focused analyzer and integration tests cover these boundaries.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
