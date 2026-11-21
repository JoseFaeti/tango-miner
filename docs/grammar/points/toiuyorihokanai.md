# 〜と言うよりほかない

[Meaning](#meaning) · [Implementation](#implementation)

Status: [~]

## Meaning

Can only describe or characterize something in the stated way.

## Implementation

The rule matches と言うより or というより followed by ほか or 他,
optionally は, and a negative predicate. Plain past and polite negatives are
supported. The span starts at と, leaving the preceding description available
as lexical evidence. The stable identity is `〜と言うよりほかない`
(`toiuyorihokanai`).

Focused Sudachi tests cover spelling and inflection variants, unrelated
comparisons, intervening text, reservation and the separate
`〜ほかない / 〜しかない` rule. Matching is morphological; a literal
comparison can remain ambiguous. Other spellings, merged tokens and intervening
words are not covered.

- [Rule](../../../src/grammar_rules/rules/toiuyorihokanai/index.py)
- [Tests](../../../src/grammar_rules/rules/toiuyorihokanai/test_index.py)
