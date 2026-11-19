# 〜べくして

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

An event happens as was inevitable or to be expected.

## Tracked scope

Recognizes adjacent verbal dictionary forms followed by auxiliary `べく` and
`して`, highlighting `べくして` while retaining the verb. Tests cover Sudachi
forms, lexical controls, boundaries and card creation, including the distinct
purpose construction `〜べく`. The matcher does not verify that the following
event repeats the verb or establish inevitability from context. Sudachi analyzes
`なるべく` as a single adverb, so `なるべくしてなった` is missed; splitting it
would require resolving its lexical ambiguity. Other tokenizations may also be
missed. Corpus precision and recall have not been assessed.

## Implementation

- Rule and tests: [bekushite](../../../src/grammar_rules/rules/bekushite/)
- Registry identity: `bekushite`; stable identity: `〜べくして`.
