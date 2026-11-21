# 〜というものではない

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Not necessarily.

## Tracked scope

Matches adjacent `というものではない` after a preceding token, including `言う`,
`っていう`, `じゃない`, polite and past negatives, and `ないです`. The card
span begins at `と` or `って`. Sudachi full-dictionary tests cover these forms,
lexical controls, and precedence over the shorter `という` and `ものではない`
rules. Context may also allow a literal naming interpretation; the matcher does
not disambiguate it. Other tokenizations and conjugations, `もん`, and the
separate `というものでもない` remain outside this scope. Corpus precision and
recall have not been evaluated.

## Implementation

- Rule and tests: [toiumonodehanai](../../../src/grammar_rules/rules/toiumonodehanai/)
- Registry identity: `toiumonodehanai`; stable identity: `〜というものではない`.
