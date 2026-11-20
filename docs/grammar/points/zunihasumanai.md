# 〜ずにはすまない

- [Meaning](#meaning)
- [Implemented behavior](#implemented-behavior)

Status: [~]

## Meaning

Cannot get away without doing something; must do it to settle a situation.

## Implemented behavior

Matches an irrealis verb or supported causative/passive auxiliary followed by
ずには and negative 済む or すむ, including past, continuative, conditional
and polite negative forms. The span starts at ず, preserving the verb as
lexical evidence. A standalone verb also produces the shorter 〜ずに card.
Tests cover Sudachi forms, lexical controls, reservation and overlapping cards.

Other tokenizations and corpus precision and recall remain unverified. Kana
すむ can be ambiguous with 住む if the analyzer selects the 済む lemma.
