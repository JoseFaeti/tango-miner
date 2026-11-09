# 〜ない

Status: [~]

## Meaning and tracked scope

verbal negation.

  - Covered: supported split negative auxiliaries after irrealis verbs and
    selected passive, potential and causative auxiliaries.
  - Remaining: adjective negation, polite and other forms, merged analyses
    and independent precision and recall evaluation.

## Implementation

- Rule: [src/grammar_rules/rules/nai/index.py](../../../src/grammar_rules/rules/nai/index.py)
- Tests: [tests/test_grammar_nai.py](../../../src/grammar_rules/rules/nai/test_index.py)
- Registry identity: `verbal_nai`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ない` card covers split verbal negative auxiliaries after an irrealis
verb or selected passive, potential and causative auxiliaries. It includes
`ない`, `なかった`, `なかったら`, `なく`, `なきゃ`, `なけりゃ`, split
`なければ`, and the observed split `なくっ` + `たって`; following `で` and
`て` remain outside the span. Independent adjectival `ない` and `無い`
remain lexical, and adjective negation, polite negatives and merged analyses
are not covered. Complete `なくったっていい` takes precedence as no obligation.
Focused tests cover incomplete and nonadjacent sequences; corpus-wide precision,
recall and expression precedence remain unevaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
