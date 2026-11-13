# 〜一方だ

Status: [~]

## Meaning

Continual change in one direction.

## Implementation

- Rule: [itsupouda/index.py](../../../src/grammar_rules/rules/itsupouda/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/itsupouda/test_index.py)
- Registry identity: `itsupouda`; expression: `〜一方だ`.

The rule matches a terminal or attributive verb directly followed by the noun
一方 and だ, です, だった or でした. Sudachi full-dictionary tests cover
these forms and distinguish 一方で, 一方通行, past verbs and adjectives.
The grammar span reserves 一方 and its copula; the preceding verb remains
available as lexical evidence. Shared dictionary-expression precedence applies.

Coverage is partial: other copular forms and noun-modifying 一方の are not
matched. Morphology alone cannot establish continual change: a verb followed by
一方だ can describe a contrasting side or one-sided action, so matches require
contextual review. Conversely, analyzer boundary or POS differences can miss
valid examples.
