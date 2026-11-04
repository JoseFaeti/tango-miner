# 〜てから

Status: [~]

## Meaning

After doing; since doing.

## Implementation

- Rule and tests: [te_kara](../../../src/grammar_rules/rules/te_kara/index.py), [tests](../../../src/grammar_rules/rules/te_kara/test_index.py).
- Registry identity: `te_kara`; expression `〜てから`.

The rule recognizes an adjacent continuative verb or auxiliary followed by
connective て or で and the particle から. It reserves only てから or
でから, retaining the preceding verb as lexical evidence. Tests exercise both
forms, elapsed time, controls for causal and spatial から, token boundaries,
and card reservation. Split forms were checked with Sudachi's full dictionary.

The temporal sequence and elapsed-time interpretations share one card; the
rule does not distinguish them. Merged or differently tokenized forms are not
covered, and wider corpus precision and recall have not been assessed.

Reservation can suppress longer lexical candidates. In the source sentence
「劫火と戦ってからというもの自分の力に自信がついた気がする。」,
the unreserved merger produces からというもの; the rule reserves て and
から and the remainder merges as というもの. This loses the complete
expression candidate, even though the 〜てから card remains. Preserving both
analyses is unresolved. Similarly, 「確認してからにする」 formerly merged
からにする, but that dictionary entry means “to empty” and does not describe
this example; reservation leaves に and する outside the grammar span.

In the compared archives, the unresolved review candidate からに fell from
43 occurrences to 7. For 「勝ってからにしな」, the earlier merger formed
からに and the new rule reserves て and から. For 「明日からに備えよう」,
neither version reserves those tokens and both merge からに. The archives
lack occurrence diagnostics, so these examples do not establish how all 36
lost occurrences should be analyzed or whether useful readings were lost.
