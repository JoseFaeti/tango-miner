# 〜と言う / 〜と聞く

Status: [~]

## Meaning

Quotation or reported information.

## Implemented scope

The `to_iu_kiku` rule recognizes a contiguous finite verb, adjective, or
auxiliary followed by と and 言う or 聞く, including supported plain, past,
polite, negative, and connective forms. The grammar span begins at と, so the
quoted predicate remains available as a lexical candidate. Attributive という
before a noun belongs to the separate [〜という / 〜っていう](toiu-tsuteiu.md)
rule. Independent lexical uses of 言う and 聞く remain eligible. Meaning is
context-dependent and the matcher does not classify it semantically.

Quoted questions, bare noun predicates, quoted dialogue with punctuation,
colloquial って, and other inflections remain unsupported. Tests exercise
Sudachi tokenization, lexical controls, attribution, and reservation in
[test_index.py](../../../src/grammar_rules/rules/to_iu_kiku/test_index.py).
See [stage and precedence](../stage-and-precedence.md) for shared behavior.
