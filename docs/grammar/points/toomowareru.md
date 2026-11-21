# 〜と思われる

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Remaining limitations](#remaining-limitations)
- [Implementation](#implementation)

Status: [~]

## Meaning

It is thought or believed that; an inference.

## Verified coverage

Matches an adjacent finite verb, adjective, auxiliary or noun followed by
`と思われる`, including positive past and polite forms. The span starts at
`と`, leaving the preceding predicate or noun available. Full-dictionary
tests cover inflections, spacing and other-verb controls. Active matches
reserve their tokens; dictionary expression precedence can suppress them.
`ものと思われる` is left to its separate rule.

## Remaining limitations

Morphology cannot distinguish all literal or other readings. Negative and
other inflections, alternate spellings and merged analyses are unsupported.
Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [toomowareru](../../../src/grammar_rules/rules/toomowareru/)
- Registry identity: `toomowareru`; stable identity: `〜と思われる`.
