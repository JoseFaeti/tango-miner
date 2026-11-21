# 〜というわけではない

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Not necessarily.

## Verified coverage

Matches finite predicates followed by `というわけではない`, including `と言う`,
`っていう`, `訳`, `じゃない`, past and polite negatives, and `ないです`.
The construction is reserved while the preceding predicate remains available;
the shorter という and わけではない rules yield to it. Focused tests cover
Sudachi tokenization, controls, reservation and card identity. Literal quotation
can be ambiguous. Nominal predicates without a copula, other tokenizations and
contractions such as ってわけではない remain unsupported. Corpus precision and
recall have not been evaluated.
