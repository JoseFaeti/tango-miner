# 〜とみられる

Status: [~]

## Meaning

It is considered or believed that; an inference.

## Verified coverage

Matches an adjacent finite verb, adjective, auxiliary or noun followed by
`と見られる` or `とみられる`, including positive past and polite forms. The
span starts at `と`, leaving the preceding predicate or noun available.
Analyzer tests cover both spellings, inflections, spacing and other-verb
controls. Active matches reserve their tokens; dictionary expression
precedence can suppress them. `ものと見られる` is left to its separate rule.

## Remaining limitations

Noun and predicate attachments can also have literal or other readings;
morphology alone cannot resolve the intended sense. Negative and other
inflections and merged analyses are unsupported. Corpus precision and recall
have not been evaluated.

## Implementation

- Rule and tests: [tomirareru](../../../src/grammar_rules/rules/tomirareru/)
- Registry identity: `tomirareru`; stable identity: `〜とみられる`.
