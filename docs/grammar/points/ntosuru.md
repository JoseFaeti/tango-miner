# 〜んとする

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

About to or intending to; literary.

## Tracked scope

Matches verbal irrealis plus `んとする`, including separate and merged Sudachi
forms and supported inflections of `する`. The lexical verb is preserved; when
`ん` is merged into it, whole-token reservation cannot reserve that token.
The same span is excluded from the negative `〜ぬ / 〜ん` rule. Focused tests
cover analyzer forms, controls, boundaries, reservation and card identity.
Standalone `せん` and other ambiguous analyzer forms may be missed. Context can
affect interpretation; corpus precision and recall have not been assessed.

## Implementation

- Rule and tests: [ntosuru](../../../src/grammar_rules/rules/ntosuru/)
- Registry identity: `ntosuru`; stable identity: `〜んとする`.
