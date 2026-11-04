# 〜なら

Status: [~]

## Meaning and tracked scope

conditional or topic-based supposition. Covered: split copular なら after selected finite predicates, nouns, pronouns, na-adjectives and predicate + の, with a following clause. Other analyses and independent precision and recall evaluation remain.

## Implementation

- Rule: [src/grammar_rules/rules/nara/index.py](../../../src/grammar_rules/rules/nara/index.py)
- Tests: [tests/test_grammar_nara.py](../../../src/grammar_rules/rules/nara/test_index.py)
- Registry identity: `conditional_nara`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

### 〜なら

The card matches split hypothetical copular なら after an adjacent finite
predicate, noun, pronoun, na-adjective or supported predicate + の, when a
following clause is present. The antecedent remains lexical; longer dictionary
expressions can take precedence. Sentence-final uses, merged tokenizations and
other analyses remain unclassified. Topic and conditional readings are context
dependent; precision and recall have not been independently evaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
