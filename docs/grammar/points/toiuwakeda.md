# 〜というわけだ

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

That is why; it follows that.

## Verified coverage

Matches finite predicates followed by `というわけだ`, including `と言う`,
`っていう`, `訳`, polite `です`, and past `だった` / `でした`. The span
reserves the construction while retaining the preceding predicate; the shorter
という and わけだ rules yield to it. Dictionary-expression precedence applies.
Focused tests cover analyzer tokenization, controls, reservation and card identity.
Literal quotations can be ambiguous. Nominal predicates without a copula, other
inflections and contractions such as ってわけだ remain unsupported. Negative
というわけではない is a separate point.
