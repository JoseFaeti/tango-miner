# 〜ということは

Status: [~]

## Meaning

Implication.

## Verified coverage

Matches a finite predicate followed by `ということは`, including `と言う`,
`っていう` and `事`. The construction is reserved without its preceding
predicate, and the shorter という and って rules yield to it. Focused tests
cover analyzer tokenization, forms, controls, reservation and card identity.
The surface can also introduce a literal topic, so implication is not guaranteed.
Nominal predicates without a copula and contractions such as `ってことは` are
not supported. Immediate `ことはない` is excluded as a competing construction.
