# 〜続ける

Status: [~]

## Meaning

Continue doing an action.

## Implementation

- Rule: [tsuzukeru](../../../src/grammar_rules/rules/tsuzukeru/index.py)
- Tests: [tsuzukeru tests](../../../src/grammar_rules/rules/tsuzukeru/test_index.py)
- Registry identity: `tsuzukeru`; stable expression: `〜続ける`.

Matches source-aligned verb compounds ending in 続ける or つづける, including
merged compounds and a continuative verb followed by a separate suffix. Plain,
past, te, negative, polite and conditional forms are supported. Standalone
続ける is excluded. Matches do not reserve tokens, preserving lexical candidates.

The tests cover Sudachi forms, split tokens, incomplete and nonadjacent forms,
standalone verbs, and card integration. Other inflections and analyzer
segmentations remain unverified; a verb stem analyzed as a noun may be missed.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
