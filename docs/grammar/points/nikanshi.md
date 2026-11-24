# 〜に関し

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Concerning; regarding (formal).

## Verified coverage

Matches adjacent noun or pronoun + に関し before a following clause,
punctuation, or sentence end. The noun remains lexical evidence. Analyzer-based
examples include 問題に関し説明する and それに関し、報告する.

The separate に関して / に関する rule retains longer forms, including
に関しまして. This rule excludes attached particles and auxiliaries to avoid
claiming those forms or inflected lexical 関する. Tests cover these controls,
source gaps, reservation, example surface and registered identity in
`src/grammar_rules/rules/nikanshi/test_index.py`.

Kana spellings, other tokenizations, and particle attachments remain unverified.
Evidence is from constructed Sudachi full dictionary cases; corpus coverage has
not been measured.
