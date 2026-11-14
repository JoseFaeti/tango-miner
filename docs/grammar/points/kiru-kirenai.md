# 〜きる / 〜きれない

Status: [~]

## Meaning

Do something completely, or be unable to finish doing it.

## Implementation

- Rule: [kiru_kirenai](../../../src/grammar_rules/rules/kiru_kirenai/index.py)
- Tests: [kiru_kirenai tests](../../../src/grammar_rules/rules/kiru_kirenai/test_index.py)
- Registry identity: `kiru_kirenai`; stable expression: `〜きる / 〜きれない`.

Matches a continuative verb followed by きる / 切る or negative potential
きれる / 切れる, and kanji-spelled merged compounds. Supported completion
forms include plain, past, te, positive polite and conditional; inability
includes plain and polite negatives and supported negative inflections. The
span includes the preceding verb, and tokens remain available as lexical
evidence. Standalone 切る and positive potential 切れる are excluded.

Sudachi full dictionary examples include 読み切った, 食べきれない and
使い切ります. Compound forms can have lexicalized or literal meanings, so
matches need review. Merged kana compounds, other inflections and analyzer
segmentations remain unsupported; merged 切れる compounds may also have a
non-potential reading. See [stage and precedence](../stage-and-precedence.md)
and [identity and review](../identity-and-review.md) for shared behavior.
