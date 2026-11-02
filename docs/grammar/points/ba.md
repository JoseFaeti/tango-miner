# 〜ば

Status: [~]

## Meaning and tracked scope

Conditional. The card matches split conjunctive ば following an adjacent
hypothetical verb, adjective or auxiliary when a following clause is present.
The predicate remains lexical. Supported complete obligation forms use their
own card, and longer dictionary expressions can take precedence. Sentence-final
uses, merged tokenizations and other analyses remain unclassified; contextual
precision and recall have not been independently evaluated.

## Implementation

- Rule: [src/grammar_rules/rules/ba.py](../../../src/grammar_rules/rules/ba.py)
- Tests: [tests/test_grammar_ba.py](../../../tests/test_grammar_ba.py)
- Registry identity: `conditional_ba`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
