# 〜合う

Status: [~]

## Meaning

Mutual action.

## Implementation

- Rule: [au](../../../src/grammar_rules/rules/au/index.py)
- Tests: [au tests](../../../src/grammar_rules/rules/au/test_index.py)
- Registry identity: `au`; stable expression: `〜合う`.

Matches source-aligned verb compounds ending in `合う` or `あう`, including
merged compounds and a continuative verb followed by a separate suffix. Plain,
past, te, polite, negative and conditional forms are supported. The complete
compound is included in the match and lexical candidates are not reserved.
Tests cover standalone 合う and 会う as well as compound forms.

Morphology alone cannot establish mutual meaning: lexicalized compounds such as
間に合う may have other meanings and remain review signals. Other inflections,
spellings and segmentations remain unverified.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
