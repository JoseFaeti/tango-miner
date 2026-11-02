# 〜てみる

Status: [~]

## Meaning and tracked scope

try doing something.

  - Covered: adjacent continuative verb + て/で + non-independent みる,
    including selected past, negative and polite forms.
  - Remaining: other tokenizations and inflections; distinguish literal “see”
    readings and evaluate precision and recall independently.

## Implementation

- Rule: [src/grammar_rules/rules/te_miru.py](../../../src/grammar_rules/rules/te_miru.py)
- Tests: [tests/test_grammar_te_miru.py](../../../tests/test_grammar_te_miru.py)
- Registry identity: `te_miru`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜てみる` card recognizes an adjacent continuative verb + connective て/で + non-independent みる. It includes the supported present form and selected past, negative and polite forms. Standalone 見る and noun + てみる do not match. These token and POS cues do not prove that the meaning is “try doing”; a literal see reading can remain ambiguous, and unsupported forms stay lexical. The rule requires an eligible example and has not been independently evaluated for precision or recall.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
