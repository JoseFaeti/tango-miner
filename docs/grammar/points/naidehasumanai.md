# 〜ないではすまない

- [Meaning](#meaning)
- [Implemented behavior](#implemented-behavior)

Status: [~]

## Meaning

Cannot get away without doing something; must do it to settle a situation.

## Implemented behavior

Matches a verbal negative followed by では and negative 済む or すむ,
including past, continuative, conditional and polite negative forms. The span
starts at ない, preserving the verb as lexical evidence. The shorter 〜ないで
card may also be produced. Tests cover Sudachi forms, lexical controls,
reservation and overlapping cards.

Other tokenizations and corpus precision and recall remain unverified. Kana
すむ can be ambiguous with 住む if the analyzer selects the 済む lemma.
