# 〜だろうか

Status: [~]

## Meaning

Question or doubt: wondering whether something is so.

## Tracked scope

Matches adjacent Sudachi auxiliary `だろう` or `でしょう` and question
particle `か`, including direct and embedded questions. The reserved span
covers the auxiliary and particle, leaving preceding words available. Tests
cover actual Sudachi forms, lexical controls, token boundaries, reservation,
card identity and overlap with related rules.

## Limitations

The form alone does not distinguish wondering from other questions, including
possessive readings such as `私のだろうか`. The separate `〜のだろうか`
and `だろう / でしょう` cards can overlap. Other tokenizations and corpus
precision and recall remain unverified.

## Implementation

- Rule and tests: [darouka](../../../src/grammar_rules/rules/darouka/)
- Registry identity: `darouka`; stable identity: `〜だろうか`.
