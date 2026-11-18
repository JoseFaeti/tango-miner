# 〜かたわら

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Alongside a principal activity, also doing another activity.

## Implementation

The [rule](../../../src/grammar_rules/rules/katawara/index.py) matches
かたわら or 傍ら immediately after an attributive verb, as in 働くかたわら.
The [tests](../../../src/grammar_rules/rules/katawara/test_index.py) exercise
both spellings with Sudachi's full dictionary, spatial and noun-attached
controls, token boundaries, and card identity. The matched word is reserved
for grammar; the preceding verb remains available as lexical evidence.

Coverage is partial: noun + の + かたわら is excluded because it can also
mean a physical location, as in 母の傍ら. The rule does not establish whether
the surrounding clauses actually describe concurrent activities, so matches
still require contextual review. Other tokenizations are unverified.
