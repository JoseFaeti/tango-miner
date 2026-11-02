# 〜はずだ

Status: [~]

## Meaning and tracked scope

expectation or inference. Covered: predicate-attached split はず with selected present and past plain or polite copulas. Noun + の, bare, negative, attributive and other forms remain lexical; contextual ambiguity and independent precision and recall evaluation remain.

## Implementation

- Rule: [src/grammar_rules/rules/hazu_da.py](../../../src/grammar_rules/rules/hazu_da.py)
- Tests: [tests/test_grammar_hazu_da.py](../../../tests/test_grammar_hazu_da.py)
- Registry identity: `hazu_da`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

### 〜はずだ

The expectation or inference card covers source-adjacent finite verbs, adjectives and auxiliaries followed by split はず and だ / です, だった / でした. The predicate remains lexical; はず and the copula are reserved subject to dictionary expression precedence. Noun + の is left lexical because a literal noun reading can have the same analysis. Bare, negative, attributive and other forms remain unsupported. A predicate may modify a literal noun, so contextual ambiguity remains; precision and recall have not been independently evaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
