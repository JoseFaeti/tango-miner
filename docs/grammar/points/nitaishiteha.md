# 〜に対しては

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Limitations](#limitations)

Status: [~]

## Meaning

As for; toward; in contrast to, with topical or contrastive は.

## Verified coverage

Matches adjacent nouns, pronouns, or nominalizing の + に対しては and
polite に対しましては. Supports split て + は and combined ては tokens.
The full marker is reserved while the attached nominal remains lexical evidence.
The shorter [〜に対して](nitaishite.md) identity continues to match independently
when its tokenization permits; overlapping cards follow existing aggregation.

`src/grammar_rules/rules/nitaishiteha/test_index.py` verifies real Sudachi full
forms, nominalized attachment, polite forms, synthetic tail variants, gaps,
lexical controls, card identity, frequency, example spans, and reservation.

## Limitations

Kana spellings and other attachments remain unverified. The matcher does not
separate literal 対する from grammatical uses or classify the two meanings.
Corpus recall has not been measured; no corpus pipeline was run for this change.
