# 〜てくる

Status: [~]

## Meaning and tracked scope

movement toward, return or emerging change.

  - Covered: supported split forms including past, negative, polite, て-form
    and volitional forms.
  - Remaining: merged expressions and ambiguous 〜てく; distinguish meanings
    and preserve lexical expressions.

## Implementation

- Rule: [src/grammar_rules/rules/te_kuru.py](../../../src/grammar_rules/rules/te_kuru.py)
- Tests: [tests/test_grammar_te_kuru.py](../../../src/grammar_rules/rules/tests/test_grammar_te_kuru.py)
- Registry identity: `te_kuru`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜てくる` card describes movement toward a point, an action
followed by return, or a change emerging or continuing up to the present,
depending on context. It requires an adjacent continuative verb or auxiliary,
connective て/で and non-independent くる/来る. Supported split forms include
plain, past, negative, polite, て-form and volitional forms. It does not
recognize merged words such as `持ってくる` or ambiguous `てく`, which can
also represent `ていく`. Morphology alone cannot distinguish the meanings.
Other occurrences remain available as lexical candidates and existing notes with this identity are
protected like the other grammar notes.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
