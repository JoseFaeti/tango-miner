# しか〜ない

Status: [~]

## Meaning

Only.

## Implementation

The rule recognizes the source-aligned adverbial particle しか followed locally
by a plain or polite negative predicate, including past forms and 待つしかない.
It reserves only しか, retaining the predicate and surrounding lexical evidence.
The stable card identity is `しか〜ない` (`shikanai`).

Focused analyzer tests cover plain and polite negatives, past forms, lexical
controls, and clause boundaries. The local search does not resolve longer or
intervening phrases, all negative variants, or clause structure; unrelated
nearby negatives can still cause false matches. Merged analyzer tokens are not
matched. These limitations remain for further work.

Verb + しかない may also produce the separate no-choice card
[〜ほかない / 〜しかない](hokanai-shikanai.md).

- [Rule](../../../src/grammar_rules/rules/shikanai/index.py)
- [Tests](../../../src/grammar_rules/rules/shikanai/test_index.py)
