# さえ / すら

Status: [~]

## Meaning

Even; さえ can also mark a sufficient condition.

## Implementation

The rule recognizes source-aligned Sudachi adverbial particles さえ and すら
attached to a preceding token, reserving the particle alone and preserving
surrounding lexical evidence. The stable identity is `さえ / すら` (`sae_sura`).
Analyzer tests cover both particles, case particles, noun and verb conditional
uses, and lexical さえずる, 冴える and すらすら.

The matcher does not distinguish the sufficient-condition sense of さえ from
“even” or identify the whole conditional construction. Particles merged into
larger analyzer tokens and particles separated from their host by whitespace
are not matched.

- [Rule](../../../src/grammar_rules/rules/sae_sura/index.py)
- [Tests](../../../src/grammar_rules/rules/sae_sura/test_index.py)
