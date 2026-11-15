# の / ん

Status: [~]

## Meaning

Explanatory question or ending.

## Tracked scope

Matches adjacent terminal or attributive verbs, adjectives and auxiliaries
followed by a bare sentence-final `の` or `ん`, at the end of the token stream
or before sentence or quotation punctuation. The span is the marker alone;
the predicate remains available as vocabulary. Tests cover Sudachi analyses of
questions, statements, adjectives and `な` predicates, plus a split `ん`
analysis, spacing, lexical controls and card identity. The existing `〜の`
rule can also match the same nominalizing `の`.

Nominalized fragments can look identical to explanatory endings. Following
sentence-final particles, other tokenizations and corpus precision and recall
remain unassessed. The separate `〜んだ` and `〜のか / 〜んか` rules cover
longer constructions.

## Implementation

- Rule and tests: [no_n](../../../src/grammar_rules/rules/no_n/)
- Registry identity: `no_n`; stable identity: `の / ん`.
