# 2. Recover kana-written compounds without inventing a target

[Contents](../recommended-improvements-in-priority-order.md)


Game-script text often writes ordinary words in kana. For example, `さいだん`
can be split into `さい` and `だん`; emitting the first fragment is not useful.

Allow a multi-token kana sequence to become a candidate only when its target is
grounded in the source surface. If a reliable compound cannot be recovered,
retain the candidate but mark it as ambiguous rather than suppressing it.

**Status (2026-10-26): partially implemented.** Dictionary compound lookup now
uses the exact combined source surface rather than concatenated token lemmas.
This prevents unrelated targets such as `おまつ` from polite `おまち` and
`ぬし` from `んし`, while preserving the source-derived candidate. A narrow
noun-plus-suffix reconstruction is described below; broader compound
reconstruction remains future work. Component provenance is now
retained as described below; it does not change merge decisions.

