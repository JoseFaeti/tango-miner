# 〜と言っても過言ではない

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

It is no exaggeration.

## Tracked scope

Matches adjacent `と言っても` or `といっても`, `過言`, and `ではない` or
`じゃない`, including past and polite negatives. The span begins at `と`;
the preceding claim remains available as lexical evidence. Full-dictionary
tests cover forms, controls, reservation and overlapping negative copulas.
Punctuation within the expression and merged token analyses are unsupported.
Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [toitsutemokagondehanai](../../../src/grammar_rules/rules/toitsutemokagondehanai/)
- Registry identity: `toitsutemokagondehanai`; stable identity: `〜と言っても過言ではない`.
