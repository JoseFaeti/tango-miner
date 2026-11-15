# 〜ようではないか

Status: [~]

## Meaning

An emphatic invitation or proposal: “let us” or “why don't we”.

## Tracked scope

The matcher recognizes a Sudachi volitional verb followed by `ではないか`,
`じゃないか`, or `ではありませんか` (also `じゃありませんか`). The card
example starts at the copula, preserving the complete volitional verb as lexical
evidence. It requires adjacent tokens and a final question particle; quoted
invitations are supported. Tests cover the forms, reservation and lexical
evidence, and distinguish the conditional or apparent-state `ようでは` and
ordinary `ではないか` from invitations.

The surface form alone cannot establish a speaker's intent. Other analyzer
segmentations and corpus precision and recall remain unassessed.

## Implementation

- Rule and tests: [youdehanaika](../../../src/grammar_rules/rules/youdehanaika/)
- Registry identity: `youdehanaika`; stable identity: `〜ようではないか`.
