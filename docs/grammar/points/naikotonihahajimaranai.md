# 〜ないことには始まらない

- [Meaning](#meaning)
- [Implemented behavior](#implemented-behavior)

Status: [~]

## Meaning

Nothing can begin or progress unless the condition is met.

## Implemented behavior

Matches adjacent `ないことには` followed by negative `始まる` or `はじまる`,
including past, conditional, continuative and polite negative forms. The span
starts at ない, preserving the preceding predicate as lexical evidence. The
shorter 〜ないことには card can also be produced. Tests cover Sudachi forms,
nearby constructions, reservation, registry identity and overlapping cards.

Other tokenizations and corpus precision and recall remain unverified. The
matcher does not distinguish idiomatic uses from literal negative 始まる.
