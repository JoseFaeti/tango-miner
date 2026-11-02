# 〜ても

Status: [~]

## Meaning and tracked scope

concession, “even if”. Covered: split て/で + も after continuative verbs, adjectives and auxiliaries with a following clause; supported 〜てもいい permission forms are excluded. Copular, merged and sentence-final forms and contextual ambiguity remain; precision and recall require independent evaluation.

## Implementation

- Rule: [src/grammar_rules/rules/te_mo.py](../../../src/grammar_rules/rules/te_mo.py)
- Tests: [tests/test_grammar_te_mo.py](../../../tests/test_grammar_te_mo.py)
- Registry identity: `te_mo`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

### 〜ても (concession)

Split て/で + も following a continuative verb, adjective or auxiliary and a following clause can yield “even if; even though”. The attached predicate remains available as vocabulary. Supported 〜てもいい permission forms use their own card. Copular でも, merged and sentence-final analyses are not covered; other meanings and contextual ambiguity remain, and precision and recall have not been independently evaluated. Dictionary expression precedence can suppress a match and reservation can affect lexical frequencies.


See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
