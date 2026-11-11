# 〜ほうがいい

Status: [~]

## Meaning

Advice.

## Supported behavior and ambiguity

The rule matches an adjacent non-past attributive verb, including passive and
causative auxiliaries, followed by `方`/`ほう` + `が` + `いい`/`よい`/`良い`.
The span starts at `方`/`ほう`, preserving the verb for lexical processing.
Past and negative advice is handled by the separate
[〜たほうがいい / 〜ないほうがいい](tahougaii-naihougaii.md) card.
Comparisons such as choosing between actions have the same form and are not
distinguished from advice. Inflected adjectives, other particles and alternate
tokenizations remain unsupported. A complete dictionary expression can take
precedence.

## Implementation and evidence

- Rule: [hou_ga_ii](../../../src/grammar_rules/rules/hou_ga_ii/index.py).
- [Tests](../../../src/grammar_rules/rules/hou_ga_ii/test_index.py) cover
  Sudachi forms, controls, reservation, identity and overlap with the other card.
