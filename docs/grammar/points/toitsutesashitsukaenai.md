# 〜といって差し支えない

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

It is fair to say; there is no objection to describing something this way.

## Tracked scope

Matches adjacent `と言って` or `といって`, optionally `も`, followed by
`差し支えない`, including past and polite negatives and the spelling
`差しつかえない`. The span starts at `と` and leaves the preceding claim
available as lexical evidence. Full-dictionary tests cover forms, controls,
reservation and examples. Conditional and continuative negatives, punctuation
within the expression and merged token analyses are unsupported. The literal
permission to say something can have the same form; the rule cannot distinguish
that reading. Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [toitsutesashitsukaenai](../../../src/grammar_rules/rules/toitsutesashitsukaenai/)
- Registry identity: `toitsutesashitsukaenai`; stable identity: `〜といって差し支えない`.
