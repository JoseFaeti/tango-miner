# 〜べからざる

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Cannot; must not, in literary attributive usage. JMdict entry 2018300
provides both glosses.

## Tracked scope

Recognizes adjacent verb + `べから` + `ざる` in Sudachi split mode C,
including `する`. The span excludes the verb, retaining lexical evidence.
The existing [〜べからず](bekarazu.md) card also matches this form and both
cards can be emitted. Focused tests cover analyzer forms, boundaries, lexical
controls, reservation and card identity. The matcher does not require a
following noun or distinguish contextual meanings. Other tokenizations and
corpus precision and recall remain unverified; dictionary-expression precedence
can suppress a match.

## Implementation

- Rule and tests: [bekarazaru](../../../src/grammar_rules/rules/bekarazaru/)
- Registry identity: `bekarazaru`; stable expression: `〜べからざる`.
