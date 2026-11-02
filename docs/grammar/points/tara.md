# 〜たら

Status: [~]

## Meaning and tracked scope

conditional or subsequent event.

  - Covered: split hypothetical たら / だら following supported continuative
    predicates and a following clause.
  - Remaining: sentence-final and other tokenizations, semantic distinctions,
    and independent precision and recall evaluation.

## Implementation

- Rule: [src/grammar_rules/rules/tara.py](../../../src/grammar_rules/rules/tara.py)
- Tests: [tests/test_grammar_tara.py](../../../tests/test_grammar_tara.py)
- Registry identity: `conditional_tara`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜たら` card recognizes the hypothetical auxiliary `たら` / `だら`
after an adjacent continuative verb, adjective, or auxiliary when another clause
begins afterward. It covers conditions and events followed by a later outcome;
it does not distinguish these meanings. Conjunction analyses, sentence-final
uses, and other tokenizations remain unrecognized. The rule has not undergone
independent precision or recall evaluation.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.

Evidence: [Conditional grammar and lexical boundaries](../../quality/conditional-grammar-and-lexical-boundaries-2026-10-29.md).
