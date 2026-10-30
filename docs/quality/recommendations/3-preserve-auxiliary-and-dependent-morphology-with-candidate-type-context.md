# 3. Preserve auxiliary and dependent morphology with candidate-type context

[Contents](../recommended-improvements-in-priority-order.md)


Grammar forms such as `てく`, `ないで`, `られる`, and `ちゃう` are in scope.
Their POS should be preserved as candidate-type context so that definitions and
presentation can improve without filtering them out.

**Status (2026-10-27): partial.** Merged tokens retain immutable component
surfaces, lemmas, base forms, readings, and POS. Candidate aggregation counts
distinct component sequences, including unmerged tokens. Some definition and
parsing corrections use these counts. Optional occurrence diagnostics now
export the individual source-linked analyses and final token rule, including
tokens excluded before aggregation; candidate diagnostics provide final status
and definition. Optional diagnostics also record rejected dictionary matches considered by the greedy merger and the selected JMdict entry when available. These records do not change candidate eligibility; skipped alternatives and derived potential definitions are not represented as selected entries.

