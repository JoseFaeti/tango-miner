# 〜ほかない / 〜しかない

Status: [~]

## Meaning

Have no choice but to do something.

## Implementation

The rule recognizes a terminal or attributive verb followed directly by
しかない, ほかない, 他ない, ほかはない, or 他はない. Plain past and
polite negative forms are supported. The matched span starts at しか or
ほか and leaves the verb available as lexical evidence. The stable card
identity is `〜ほかない / 〜しかない` (`hokanai_shikanai`).

Focused Sudachi tests cover these forms and noun-only, unrelated negative,
and clause controls. The existing broader `しか〜ない` card also matches
verb + しかない and both cards are retained on overlap. Matching is
morphological: context can make an apparent no-choice reading ambiguous.
Intervening words, merged tokens, and other variants are not yet covered.

- [Rule](../../../src/grammar_rules/rules/hokanai_shikanai/index.py)
- [Tests](../../../src/grammar_rules/rules/hokanai_shikanai/test_index.py)
