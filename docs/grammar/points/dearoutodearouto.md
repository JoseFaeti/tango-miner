# 〜であろうと〜であろうと

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Whether one alternative or another.

## Verified coverage

Recognizes two copular であろうと endings with intervening material, including
a comma. Each ending is reserved separately, preserving the alternatives as
lexical candidates. The existing 〜であれ / 〜であろうと rule also matches.
Focused tests cover Sudachi tokenization, single and mixed endings, sentence
boundaries, lexical controls, reservation and identity.

The matcher uses proximity within a sentence, not a semantic analysis of the
alternatives, so unrelated occurrences can match. Mixed であれ forms, other
tokenizations, and independent precision and recall remain unverified.
