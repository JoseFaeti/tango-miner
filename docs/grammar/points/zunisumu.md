# 〜ずにすむ

Status: [~]

## Meaning

Avoid having to do something.

## Implemented behavior

Matches a verb in the irrealis form followed by ずに and 済む or すむ,
including supported past, polite, conditional and negative inflections of
済む. The span starts at ず, preserving the verb as lexical evidence. The
shorter 〜ずに card is also recorded. Tests cover Sudachi forms, lexical
controls, reservation and overlapping card examples.

Other tokenizations and corpus precision and recall remain unverified. The
related 〜ずにはすまない construction is a separate point. Kana すむ may
be ambiguous with 住む when the analyzer selects the 済む lemma.
