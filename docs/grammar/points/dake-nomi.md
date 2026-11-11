# だけ / のみ

Status: [~]

## Meaning

Only.

## Implementation

The rule recognizes source-aligned Sudachi adverbial particles だけ and のみ
attached to a preceding token, including nouns and verbs. It reserves the
particle alone and preserves the surrounding lexical evidence. The stable card
identity is `だけ / のみ` (`dake_nomi`).

Focused analyzer tests cover both particles, inflected contexts, lexical
できるだけ and 酒飲み, and だけど. The matcher does not distinguish the
degree or extent sense of だけ from “only”, or isolate longer constructions
such as だけでなく. Analyzer tokens that merge the particle into a larger word
are not matched. These ambiguities and missing forms remain for further work.

- [Rule](../../../src/grammar_rules/rules/dake_nomi/index.py)
- [Tests](../../../src/grammar_rules/rules/dake_nomi/test_index.py)
