# 〜ようだ / 〜みたいだ

Status: [~]

## Meaning and tracked scope

resemblance or inference. Covered: 〜ようだ with selected predicate and noun attachments and present or past plain/polite copulas. 〜みたいだ, negative, attributive and continuative forms and other analyses remain unsupported; evaluate contextual meanings and recall.

## Implementation

- Rule: [src/grammar_rules/rules/you_da.py](../../../src/grammar_rules/rules/you_da.py)
- Tests: [tests/test_grammar_you_da.py](../../../tests/test_grammar_you_da.py)
- Registry identity: `you_da`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ようだ` card recognizes selected finite predicate or noun + の attachments followed by split よう + だ / です, including だった and でした. It covers inference and resemblance without distinguishing them. Negative, attributive and continuative forms, みたいだ, other tokenizations and demonstrative このようだ remain lexical; independent precision and recall evaluation remains needed.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
