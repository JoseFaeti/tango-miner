# 〜方

Status: [~]

## Meaning

Way of doing.

## Implementation

- Rule: [hou](../../../src/grammar_rules/rules/hou/index.py)
- Tests: [hou tests](../../../src/grammar_rules/rules/hou/test_index.py)
- Registry identity: `hou`; stable expression: `〜方`.

An adjacent verb in the general continuative form followed by the nominal
suffix `方` or `かた` matches the suffix, as in `食べ方` and `読みかた`.
Lexical candidates remain available. Tests check source evidence, token
boundaries and unrelated uses of 方. Sudachi full mode C also analyzes common
forms such as `読み方`, `やり方` and `生き方` as single nouns; these are not
matched by this rule. Morphology alone cannot resolve every meaning.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
