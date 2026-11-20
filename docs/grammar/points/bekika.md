# 〜べきか

## Index

- [Meaning and tracked scope](#meaning-and-tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning and tracked scope

Whether one should, or what one should do. Matches a source-adjacent finite verb
followed by べきか, including するべきか and すべきか. The verb remains lexical;
the suffix is reserved subject to dictionary expression precedence.

## Implementation

- Rule and tests: [bekika](../../../src/grammar_rules/rules/bekika/index.py), [tests](../../../src/grammar_rules/rules/bekika/test_index.py).
- Registry identity: `bekika`. Sudachi full dictionary examples cover direct and embedded questions, かどうか, separated tokens and lexical controls.

Fused tokenizations and other forms such as べきではないか are not covered by
this rule. Contextual precision and corpus recall have not been independently
evaluated. See [stage and precedence](../stage-and-precedence.md).
