# 〜ていく

Status: [~]

## Meaning and tracked scope

movement away or change continuing onward.

  - Covered: supported split forms including past, negative, polite, て-form
    and volitional forms.
  - Remaining: merged expressions and ambiguous 〜てく; evaluate precision and recall.

## Implementation

- Rule: [src/grammar_rules/rules/te_iku.py](../../../src/grammar_rules/rules/te_iku.py)
- Tests: [tests/test_grammar_te_iku.py](../../../tests/test_grammar_te_iku.py)
- Registry identity: `te_iku`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ていく` card recognizes supported split verb + connective + いく /
行く / ゆく forms, including past, negative, polite, て-form and volitional
forms. It covers movement away and continuing change without distinguishing
those meanings. Merged expressions and ambiguous contracted `〜てく` remain
unrecognized; independent precision and recall evaluation remains needed.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
