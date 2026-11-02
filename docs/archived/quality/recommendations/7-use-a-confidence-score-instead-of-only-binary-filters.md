# 7. Use a confidence score instead of only binary filters

[Contents](../recommended-improvements-in-priority-order.md)


Combine existing signals into a candidate-confidence score:

- POS reliability
- kana-only and word-length penalties
- agreement between source surface and lemma
- dictionary/POS/reading agreement
- confidence in compound reconstruction
- consistency across example contexts

Candidates below a threshold should remain available in normal output, with a
confidence marker or review tag where practical.

