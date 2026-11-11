# 〜てくれない / 〜てもらえない

Status: [~]

## Meaning

Informal request: could you do something for me?

## Implementation and limitations

The `te_kurenai_temoraenai` rule matches an adjacent continuative verb or
auxiliary, connective て/で, くれ or potential もらえ, and ない at the end
of a sentence or before sentence-final か. Tests cover both connective forms,
requests, nonverbal and lexical controls, incomplete forms, reservation and
card identity. The attached verb remains eligible as vocabulary, subject to
shared dictionary expression precedence. Other grammar rules can overlap.

A sentence-final negative can also be a statement or a question about a fact,
so the rule cannot establish request intent. Other inflections, sentence-final
particles, contracted and merged tokenizations, and corpus precision and recall
remain unevaluated.

See [rule](../../../src/grammar_rules/rules/te_kurenai_temoraenai/index.py),
[tests](../../../src/grammar_rules/rules/te_kurenai_temoraenai/test_index.py) and
[stage and precedence](../stage-and-precedence.md).
