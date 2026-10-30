# Targeted grammatical-boundary fixes (2026-10-27)

[Quality analysis index](../false-positive-analysis.md)


The supplied examples include `できないし` and `そんなことしてみろ`:
matching their surfaces across Sudachi tokens previously produced the unrelated
dictionary entries `ないし` (“or”) and `ことし` (“this year”). The merge
stage now rejects a conjunction-only entry spanning an auxiliary and particle,
and a noun/adverb-only entry spanning a noun and an inflected `する` stem.
The original tokens remain available; genuine standalone `ないし` and
`ことし`, and auxiliary expressions such as `ないで`, remain covered by tests.
This is a targeted improvement, not a guarantee that all suspended examples
disappear: definition ambiguity, proper names, and incorrect single-token
Sudachi analyses remain unresolved. Mixed uses of a
word may still aggregate into one card.

