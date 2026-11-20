# 〜べきだ / 〜べきではない

## Index

- [Meaning and tracked scope](#meaning-and-tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning and tracked scope

Ought to or ought not to. The rule covers a source-adjacent finite verb followed by split べき and a finite positive or negative copula, including するべき and すべき. The verb remains lexical; the matched suffix is reserved subject to dictionary expression precedence.

## Implementation

- Rule and tests: [bekida_bekidehanai](../../../src/grammar_rules/rules/bekida_bekidehanai/index.py), [tests](../../../src/grammar_rules/rules/bekida_bekidehanai/test_index.py).
- Registry identity: `bekida_bekidehanai`.
- Sudachi full dictionary tests cover だ / です, だった / でした, では / じゃ with negative forms, including するべき and すべき, plus incomplete, separated and lexical controls.

Attributive べき, bare べき, other inflections and fused tokenizations remain unsupported. This morphology can also express expectation or suitability; contextual precision and corpus recall have not been independently evaluated. See [stage and precedence](../stage-and-precedence.md) for shared behavior.
