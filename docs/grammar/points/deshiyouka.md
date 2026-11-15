# 〜でしょうか

Status: [~]

## Meaning

Polite question or expression of uncertainty: “might it be?” or “I wonder whether”.

## Tracked scope

Matches adjacent Sudachi auxiliary `でしょう` and question particle `か` in
direct and embedded questions. The reserved span covers the auxiliary and
particle, leaving preceding words available. Tests cover actual Sudachi forms,
lexical controls, token boundaries, reservation, identity and related overlaps.

## Limitations

The form alone cannot distinguish a polite question from conjecture. The
`〜だろうか`, `〜のだろうか` and `だろう / でしょう` cards may overlap.
Other tokenizations and corpus precision and recall remain unverified.

## Implementation

- Rule and tests: [deshiyouka](../../../src/grammar_rules/rules/deshiyouka/)
- Registry identity: `deshiyouka`; stable identity: `〜でしょうか`.
