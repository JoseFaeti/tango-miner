# 〜てもいい

Status: [~]

## Meaning and tracked scope

permission.

  - Covered: verb continuative + merged てもいい or split て/で + も + いい/よい/良い, including supported past forms. Other readings and inflections remain lexical.

## Implementation

- Rule: [src/grammar_rules/rules/te_mo_ii.py](../../../src/grammar_rules/rules/te_mo_ii.py)
- Tests: [tests/test_grammar_te_mo_ii.py](../../../tests/test_grammar_te_mo_ii.py)
- Registry identity: `te_mo_ii`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).


## Supported behavior and ambiguity

The permission card recognizes an adjacent continuative verb followed by merged
`てもいい` or split `て/で` + `も` + `いい`, `よい` or `良い`, including
`よかった`/`良かった`. It reserves the construction while retaining the verb;
a complete dictionary expression can take precedence. Noun + `でもいい` and
negative or other inflections are left to lexical processing. Depending on context,
the construction can express acceptance or concession as well as permission.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
