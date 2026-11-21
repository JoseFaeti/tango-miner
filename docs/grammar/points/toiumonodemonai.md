# 〜というものでもない

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Not necessarily; a proposition is not universally true.

## Tracked scope

Matches adjacent `というものでもない` after a preceding token, including `言う`,
`っていう`, polite and past negatives, and `ないです`. The span begins at
`と` or `って`. Tests cover Sudachi forms, lexical controls, reservation and
precedence over `という`. Literal interpretations are not disambiguated. Other
tokenizations and conjugations and `もん` remain unverified. Corpus precision
and recall have not been evaluated.

## Implementation

- Rule and tests: [toiumonodemonai](../../../src/grammar_rules/rules/toiumonodemonai/)
- Registry identity: `toiumonodemonai`; stable identity: `〜というものでもない`.
