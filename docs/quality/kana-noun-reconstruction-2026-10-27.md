# Kana noun reconstruction (2026-10-27)

[Quality analysis index](../false-positive-analysis.md)


The merge step also recognizes an exact hiragana JMdict noun surface when
Sudachi splits it into an unchanged common noun and an unchanged noun-like
suffix. This narrowly recovers `さいだん` in both supplied examples, instead
of emitting `さい` from those occurrences. Components remain in provenance.
The rule does not reconstruct mixed-script `さい月` or split words with other
POS patterns, and a dictionary spelling alone cannot prove the intended word.

A before/after comparison tokenized all 363 example sentences in the supplied
suspension export. Eleven token streams changed: two `さいだん` examples,
`はんてい`, two `ばあさん` examples, `ばあちゃん`, `みなさん`,
`おっさん`, `じいちゃん`, and two `じょうちゃん` examples. In the
`ばあちゃん` sentence this also replaces a conflicting `ちゃんと` merge
with `ばあちゃん` followed by `と`. Regression tests cover every changed
sentence, component evidence, grammar boundaries, and restrictive conditions.
The comparison checks the effects of this rule; it does not establish that all
remaining analyses are correct, or measure impact on the full source corpus.

