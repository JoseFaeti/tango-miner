# 〜なくて済む

Status: [~]

## Meaning

Avoid having to do something; get through without something being present.

## Implemented behavior

Matches a verbal negative in なく form (including なくっ + て) or the
adjectival absence form なく, followed by て and 済む or すむ. Examples
include 行かなくて済む and 待ち時間がなくて済んだ. Supported inflections of
済む follow the shared `sumu_end` matcher. For verbal forms, the span begins at
なく, preserving the preceding verb as lexical evidence. Tests cover Sudachi
forms, absence uses, inflected 済む, lexical controls, reservation, and card
identity.

Other tokenizations and corpus precision and recall remain unverified. Kana
すむ may be ambiguous with 住む when the analyzer selects the 済む lemma.
