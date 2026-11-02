# 〜てしまう

Status: [~]

## Meaning and tracked scope

completion or unintended result.

  - Covered: supported split forms and ちゃう/じゃう contractions, including
    common inflections.
  - Remaining: other tokenizations, inflections and contextual nuances.

## Implementation

- Rule: [src/grammar_rules/rules/te_shimau.py](../../../src/grammar_rules/rules/te_shimau.py)
- Tests: [tests/test_grammar_te_shimau.py](../../../tests/test_grammar_te_shimau.py)
- Registry identity: `te_shimau`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜てしまう` card describes completion or an unintended or
regrettable result, depending on context. The rule recognizes an adjacent
continuative verb plus connective て/で and non-independent しまう, as well
as the auxiliary contractions ちゃう/じゃう. Supported forms include plain,
past, common negative and polite inflections. Matched grammar tokens are excluded from lexical processing; the grammar card requires an eligible source example and
its Anki identity is protected from updates and pruning. Other tokenizations,
inflections and nuances may be missed, and the morphology alone cannot decide
the intended nuance. In an ordered replay of the first 20,000 archived
sentences the rule found 197 occurrences, including 77 `ちゃった` and 75
`ちゃう`. This limited, selected check does not establish precision or recall.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.

Related backlog family: [Contracted completion 〜ちゃう / 〜じゃう](chiyau-jiyau.md).
