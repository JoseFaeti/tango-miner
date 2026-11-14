# 〜ないですむ

Status: [~]

## Meaning

Avoid having to.

## Implemented behavior

Matches a verbal ないで followed by 済む or すむ, including supported past,
polite, conditional and negative inflections of 済む. The span starts at
ない, retaining the preceding verb as lexical evidence. The existing ないで
rule can also recognize the shorter span; both analyses remain available.
Tests cover Sudachi forms, lexical controls, reservation and card examples.

Sudachi can analyze unspaced 行かないですんだ as ない + です + ん + だ;
that ambiguous tokenization is not matched. Other tokenizations and broader
corpus precision and recall remain unverified. The related なくて済む and
ずにすむ constructions are separate points.
