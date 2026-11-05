# 〜てみる

Status: [~]

## Meaning and tracked scope

try doing something.

  - Covered: adjacent continuative verb + て/で + non-independent みる,
    including selected past, negative, polite, connective, conditional and volitional forms.
  - Remaining: other tokenizations and inflections; distinguish literal “see”
    readings and evaluate precision and recall independently.

## Implementation

- Rule: [src/grammar_rules/rules/te_miru/index.py](../../../src/grammar_rules/rules/te_miru/index.py)
- Tests: [tests/test_grammar_te_miru.py](../../../src/grammar_rules/rules/te_miru/test_index.py)
- Registry identity: `te_miru`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜てみる` card recognizes an adjacent continuative verb + connective て/で + non-independent みる. It includes the supported present form and selected past, negative, polite, connective, conditional and volitional forms, including source-aligned みて, みれば and みよう. Standalone 見る and noun + てみる do not match. These token and POS cues do not prove that the meaning is “try doing”; a literal see reading can remain ambiguous, and unsupported forms stay lexical. Focused Sudachi regressions cover these forms; corpus frequency, expression precedence and independent precision and recall remain unevaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
