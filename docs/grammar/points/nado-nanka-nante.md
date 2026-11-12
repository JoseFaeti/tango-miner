# など / なんか / なんて

Status: [~]

## Meaning

Non-exhaustive examples, emphasis or dismissal.

## Implementation

The `nado_nanka_nante` rule recognizes source-aligned Sudachi adverbial
particle など attached to a preceding token. Attached なんか and なんて are
handled by the separate [〜なんか](nanka.md) and [〜なんて](nante.md) rules. This rule reserves
the particle alone and retains the surrounding lexical evidence. The stable
identity remains `など / なんか / なんて` for compatibility. Analyzer tests
cover noun attachments, sentence-initial forms, interrogatives and whitespace;
integration tests cover reservation and card identity.

This does not identify sentence-initial discourse uses, other tokenizations,
or distinguish examples from emphasis and dismissal. Particles merged into
larger tokens or separated from their host by whitespace are not matched.

- [Rule](../../../src/grammar_rules/rules/nado_nanka_nante/index.py)
- [Tests](../../../src/grammar_rules/rules/nado_nanka_nante/test_index.py)
