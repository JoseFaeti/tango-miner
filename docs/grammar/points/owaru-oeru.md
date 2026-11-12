# 〜終わる / 〜終える

Status: [~]

## Meaning

Completion.

## Implementation

- Rule: [owaru_oeru](../../../src/grammar_rules/rules/owaru_oeru/index.py)
- Tests: [owaru_oeru tests](../../../src/grammar_rules/rules/owaru_oeru/test_index.py)
- Registry identity: `owaru_oeru`; stable expression: `〜終わる / 〜終える`.

Matches source-aligned verb compounds ending in 終わる or 終える, including
hiragana spellings and a separate suffix after an adjacent continuative verb.
Plain, past, te, negative, polite and conditional forms are supported. The span
includes the compound; matches do not reserve tokens, preserving lexical candidates.
Standalone 終わる and 終える are excluded. Tests include Sudachi tokenizations,
standalone controls, split tokens and card integration.

Other inflections and analyzer segmentations remain unverified. A split verb stem
analyzed as a noun is missed; merged compounds are matched morphologically and
may be ambiguous.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
