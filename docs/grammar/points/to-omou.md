# 〜と思う

Status: [~]

## Meaning and tracked scope

predicate quotation and intention; reserves matched 思う while retaining independent lexical uses. Quoted questions, noun predicates and other inflections remain unsupported.

## Implementation

- Rule: [src/grammar_rules/rules/to_omou.py](../../../src/grammar_rules/rules/to_omou.py)
- Tests: [tests/test_grammar_to_omou.py](../../../tests/test_grammar_to_omou.py)
- Registry identity: `to_omou`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜と思う` card matches a source-aligned finite or volitional verb,
adjective, or auxiliary followed by と and 思う, 思った, 思います,
思いました, 思わない, 思わなかった, or 思って. It describes thought or
judgment, with intention possible after volitional forms. Quoted questions,
bare noun predicates, other inflections, and spaced token boundaries are not
covered. The full matched span supplies the example and expression precedence,
and its tokens are reserved from lexical collection. The attached predicate
and independent uses of 思う remain eligible.
The meaning remains context-dependent; this is not a semantic classifier.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
