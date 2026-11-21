# 〜とされる

Status: [~]

## Meaning

Is regarded as; it is considered that.

## Verified coverage

Matches an adjacent finite predicate, noun, pronoun or adjectival noun followed
by `とされる`, including positive past, polite and te forms. The span starts
at `と`, preserving the preceding expression. Analyzer tests cover attachments,
inflections, spacing and unrelated passive and active forms. Active matches
reserve their tokens; dictionary expression precedence can suppress them.

## Remaining limitations

The construction can have other readings that morphology cannot distinguish.
Negative and other inflections and merged analyses are unsupported. Corpus
precision and recall have not been evaluated.

## Implementation

- Rule and tests: [tosareru](../../../src/grammar_rules/rules/tosareru/)
- Registry identity: `tosareru`; stable identity: `〜とされる`.
