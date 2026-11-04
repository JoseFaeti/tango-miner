# 〜やすい / 〜にくい

Status: [~]

## Meaning

Ease or difficulty.

## Implementation

- Rule: [yasui_nikui](../../../src/grammar_rules/rules/yasui_nikui/index.py)
- Tests: [yasui_nikui tests](../../../src/grammar_rules/rules/yasui_nikui/test_index.py)
- Registry identity: `yasui_nikui`; stable expression: `〜やすい / 〜にくい`.

A source-aligned continuative verb followed by Sudachi's adjectival suffix
`やすい`, `にくい`, `易い` or `難い` produces a card. Supported forms include
plain, past, negative, conditional, continuative, te and plain plus `です`,
such as `読みやすかった`, `読みにくくない` and `分かり易い`. The matched
surface starts at the suffix; lexical candidates remain available. Tests use
Sudachi full dictionary tokenizations and check unrelated adjectives, boundaries
and source evidence. Morphology alone does not guarantee the intended meaning.
Merged verb-suffix tokens and other spellings or inflections remain unverified.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
