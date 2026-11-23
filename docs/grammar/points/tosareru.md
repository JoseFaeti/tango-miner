# 〜とされる

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Remaining limitations](#remaining-limitations)
- [Implementation](#implementation)

Status: [~]

## Meaning

Is regarded as; it is considered that.

## Verified coverage

Matches an adjacent finite predicate, noun, pronoun or adjectival noun followed
by `とされる`, including positive past, polite and te forms. The span starts
at `と`, preserving the preceding expression. Analyzer tests cover attachments,
inflections, spacing and unrelated passive and active forms. Active matches
reserve their tokens; dictionary expression precedence can suppress them.

`必要とされる` and its supported inflections are excluded as passive
`必要とする` (to need). Dictionary merging recovers that lexical expression
through the inflected `する` token and retains its original components;
passive auxiliaries remain separate. The shared control also excludes
`必要とされている`. Nouns remain eligible (`勇者のベルトとされる`,
`問題とされる`), as do clauses (`村を守るとされる`) and explicit
copular judgments (`必要だとされる`). Focused analyzer and pipeline tests
cover these distinctions, lexical preservation and grammar reservation.

## Remaining limitations

The construction can have other readings that morphology cannot distinguish.
Negative and other inflections and merged analyses are unsupported. Corpus
precision and recall have not been evaluated.

## Implementation

- Rule and tests: [tosareru](../../../src/grammar_rules/rules/tosareru/)
- Registry identity: `tosareru`; stable identity: `〜とされる`.
