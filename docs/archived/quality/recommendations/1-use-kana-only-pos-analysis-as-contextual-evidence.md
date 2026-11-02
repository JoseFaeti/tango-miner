# 1. Use kana-only POS analysis as contextual evidence

[Contents](../recommended-improvements-in-priority-order.md)


Treat kana-only POS analysis as a confidence signal. Preserve useful kana-only
lexical items, grammar, and expressions unless there is strong evidence that a
candidate target is wrong.

The suspension export showed many ambiguous kana forms such as `しよう`, `さい`,
`ひょう`, and `てく`, but the recall-first policy means they should inform
target-selection improvements rather than justify broad exclusion.

**Status (2026-10-26): reverted for recall.** Kana-only auxiliaries,
non-independent forms, and short kana-only nouns remain eligible. The pipeline
now favors preserving candidates and improving an incorrect target selection
later, rather than hiding potentially useful grammar or colloquial material.

