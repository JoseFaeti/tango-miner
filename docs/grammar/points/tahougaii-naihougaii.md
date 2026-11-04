# 〜たほうがいい / 〜ないほうがいい

Status: [~]

## Meaning

Recommendation.

## Supported behavior and ambiguity

The rule recognizes an adjacent verb or supported passive/causative auxiliary
followed by past `た`/`だ` or negative `ない`, then `ほう`/`方` + `が` +
`いい`/`よい`/`良い`. The grammar span starts at the past or negative
suffix and ends at the adjective; the attached verb remains eligible for
lexical processing. A complete dictionary expression can take precedence.
The same form can express a comparison rather than advice; the rule does not
distinguish those readings. Present affirmative verbs, inflected adjectives,
other particles, and alternative tokenizations remain unsupported.

## Implementation and evidence

- Rule: [ta_hou_ga_ii.py](../../../src/grammar_rules/rules/ta_hou_ga_ii.py),
  registry identity `ta_hou_ga_ii`.
- Tests: [test_grammar_ta_hou_ga_ii.py](../../../src/grammar_rules/rules/tests/test_grammar_ta_hou_ga_ii.py)
  cover Sudachi-analyzed positive and negative examples, lexical controls,
  boundaries, reservation and card creation.
- Shared behavior: [stage and precedence](../stage-and-precedence.md) and
  [identity and review](../identity-and-review.md).
