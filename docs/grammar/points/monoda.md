# 〜ものだ

Status: [~]

## Meaning

General truth, recollection or admonition.

## Tracked scope

The rule matches adjacent predicate + nominal `もの` + finite `だ`, `です`,
`だった`, or `でした`, including past and negative predicates when analyzed
as terminal or attributive verbs, adjectives, or auxiliaries. The span is the
`もの` and copula. Sudachi full-dictionary tests cover general truths,
recollection, admonition, polite and past forms, and a na-adjective.

This morphology also matches literal things, such as `大切なものです`; it
cannot establish the intended meaning. The rule does not reserve its tokens,
though overlapping rules may reserve them. The causal `ものだから` is excluded.
Colloquial `もんだ`, negative copulas, other inflections and merged analyses
remain unsupported. Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [monoda](../../../src/grammar_rules/rules/monoda/)
- Registry identity: `monoda`; stable identity: `〜ものだ`.
- See [stage and precedence](../stage-and-precedence.md) and
  [identity and review](../identity-and-review.md).
