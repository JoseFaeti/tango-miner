# 〜始める / 〜出す

Status: [~]

## Meaning

Beginning.

## Implementation

- Rule: [hajimeru_dasu](../../../src/grammar_rules/rules/hajimeru_dasu/index.py)
- Tests: [hajimeru_dasu tests](../../../src/grammar_rules/rules/hajimeru_dasu/test_index.py)
- Registry identity: `hajimeru_dasu`; stable expression: `〜始める / 〜出す`.

Matches source-aligned verb compounds ending in `始める`, `はじめる`, `出す`
or `だす`, including merged compounds and a continuative verb followed by a
separate suffix. Plain, past, te, negative, polite and conditional forms are
supported; the span includes the compound. Matches do not reserve tokens, so
lexical candidates remain available. Standalone 始める and 出す are excluded.

Compound morphology cannot reliably distinguish beginning from other meanings
of 出す, as in 取り出す (take out), so matches can be ambiguous. Sudachi may
analyze a verb stem as a noun, as with 笑い出さない, which is missed when split.
Other inflections and analyzer segmentations remain unverified.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
