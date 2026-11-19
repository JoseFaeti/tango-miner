# 〜べく

Status: [~]

## Meaning

In order to.

## Tracked scope

Recognizes auxiliary `べく` after an adjacent verbal dictionary form, including
`するべく` and `すべく`, and highlights the auxiliary while retaining the verb.
Tests cover Sudachi tokenization, lexical controls, boundaries and card creation.
The distinct `べくして` and `べくもない` constructions are excluded. The
matcher cannot establish purpose from syntax alone; other meanings or unusual
tokenizations may be misclassified or missed. Corpus precision and recall have
not been assessed.

## Implementation

- Rule and tests: [beku](../../../src/grammar_rules/rules/beku/)
- Registry identity: `beku`; stable identity: `〜べく`.
