# 〜直す

Status: [~]

## Meaning

Doing again.

## Implementation

- Rule: [naosu](../../../src/grammar_rules/rules/naosu/index.py)
- Tests: [naosu tests](../../../src/grammar_rules/rules/naosu/test_index.py)
- Registry identity: `naosu`; stable expression: `〜直す`.

Matches source-aligned verb compounds ending in `直す` or `なおす`, including
Sudachi's merged compounds and a continuative verb followed by a separate
`直す`. Plain, past, te, negative, polite and conditional forms are supported;
the matched span includes the whole compound. These matches are not reserved,
so their lexical candidates remain available. Tests include Sudachi full
dictionary forms and standalone `直す`.
Compound morphology does not distinguish all lexicalized meanings: for example,
`見直す` can mean reassess rather than literally do again. Other spellings,
inflections and analyzer segmentations remain unverified.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
