# Yet-to-learn card review (2026-10-27)

[Quality analysis index](../false-positive-analysis.md)


The latest completed run's displayed examples show additional definition and
target-selection problems. `により` is defined as “similarity” where its
examples express means or accordance; `おって` is defined as a pursuer where
the examples use inflected auxiliary `おる` after a verb stem; and `覆われ`
is given a noun definition where the examples use passive `覆う`. The
`つれて` examples use `連れる`, not the proportional expression; two of
three `にかけて` examples use `掛ける`, while one is the swearing expression.
These examples motivate POS and inflection-aware selection as well as
occurrence-level expression disambiguation. Preserve genuine expression and
homophone uses when adding regressions; do not reject noun entries globally.

The same run defined `する` as “rub” or “squander” and `いる` as “need”,
although their displayed examples use “do” and auxiliary `いる`. These
high-frequency homophones are particularly valuable regression cases. The
review covers the exported examples, not all occurrences. The run did not
enable occurrence diagnostics, so its CSV does not establish the underlying
token analysis or the identity of the selected dictionary entry.


