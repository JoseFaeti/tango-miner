# 〜込む

Status: [~]

## Meaning

Enter into something or do something deeply or thoroughly.

## Implementation

- Rule: [gomu](../../../src/grammar_rules/rules/gomu/index.py)
- Tests: [gomu tests](../../../src/grammar_rules/rules/gomu/test_index.py)
- Registry identity: `gomu`; stable expression: `〜込む`.

Matches an adjacent continuative verb and 込む / こむ, or a merged
kanji-spelled compound. Plain, past, te, polite, negative and conditional
forms are supported. The span includes the preceding verb and retains lexical
candidates. Standalone 込む and 混む are excluded.

Sudachi examples include 飛び込む, 書き込んだ and 考え込みます.
Compounds may have literal or lexicalized meanings and require review. Merged
kana compounds, potential forms, other inflections and analyzer segmentations
remain unsupported.
