# など / なんか / なんて

Status: [~]

## Meaning

Non-exhaustive examples, emphasis or dismissal.

## Implementation

The `nado_nanka_nante` rule recognizes source-aligned Sudachi adverbial
particles など, なんか and なんて attached to a preceding token. It reserves
the particle alone and retains the surrounding lexical evidence. The stable
identity is `など / なんか / なんて`. Analyzer tests cover noun and clause
attachments, sentence-initial なんか and なんて, interrogative forms, and
whitespace; integration tests cover reservation and card identity.

This does not identify sentence-initial discourse uses, other tokenizations,
or distinguish examples from emphasis and dismissal. Particles merged into
larger tokens or separated from their host by whitespace are not matched.

- [Rule](../../../src/grammar_rules/rules/nado_nanka_nante/index.py)
- [Tests](../../../src/grammar_rules/rules/nado_nanka_nante/test_index.py)
