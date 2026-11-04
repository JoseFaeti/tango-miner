# 〜そうだ

Status: [~]

## Meaning and tracked scope

distinct appearance and hearsay cards. Covered: selected source-aligned finite split forms; negative, attributive and other analyses remain unsupported. Contextual ambiguity and independent evaluation remain.

## Implementation

- Rule: [src/grammar_rules/rules/sou_da.py](../../../src/grammar_rules/rules/sou_da.py)
- Tests: [tests/test_grammar_sou_da.py](../../../src/grammar_rules/rules/tests/test_grammar_sou_da.py)
- Registry identities: `sou_da_appearance`, `sou_da_hearsay`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Variant-specific behavior

Appearance (`sou_da_appearance`) expresses how something looks or seems likely;
it accepts selected continuative verb, adjective stem and na-adjective analyses,
with split そうだ / そうです and supported past copulas. Hearsay
(`sou_da_hearsay`) reports information and accepts selected finite predicates
with present だ / です. Their meanings and identities remain distinct.

The attached predicate remains lexical; the suffix is reserved subject to
dictionary expression precedence. Negative and attributive forms, merged
analyses and other inflections remain unsupported. Tokenization and context can
be ambiguous; precision and recall have not been independently evaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
