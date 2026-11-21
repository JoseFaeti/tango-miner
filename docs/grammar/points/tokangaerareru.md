# 〜と考えられる

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Remaining limitations](#remaining-limitations)
- [Implementation](#implementation)

Status: [~]

## Meaning

It is considered or thought that; an inference or judgment.

## Verified coverage

Matches an adjacent finite verb, adjective, auxiliary or noun followed by
`と考えられる`, including positive past and polite forms. The span starts at
`と`, leaving the preceding predicate or noun available. Sudachi full-dictionary
tests cover these forms, spacing, active and negative controls. Active matches
reserve their tokens; dictionary expression precedence can suppress them.

## Remaining limitations

Passive and potential readings cannot always be distinguished from context.
Negative and other inflections, alternate spellings and merged analyses are
unsupported. Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [tokangaerareru](../../../src/grammar_rules/rules/tokangaerareru/)
- Registry identity: `tokangaerareru`; stable identity: `〜と考えられる`.
