# 〜すぎる

Status: [~]

## Meaning

Excess: doing something too much or having too much of a quality.

## Implementation

- Rule: [sugiru](../../../src/grammar_rules/rules/sugiru/index.py)
- Tests: [sugiru tests](../../../src/grammar_rules/rules/sugiru/test_index.py)
- Registry identity: `sugiru`; stable expression: `〜すぎる`.

Matches adjacent continuative verbs, adjective stems, and な-adjective stems
followed by すぎる or 過ぎる, and Sudachi non-independent merged verb
compounds. Plain, past, te, negative, polite and conditional forms are
supported. The span covers the suffix when separate and the entire compound
when merged. Matches do not reserve lexical candidates. Tests use Sudachi full
and check standalone 過ぎる and lexical compounds such as 通り過ぎる.

Other inflections, nominalized forms and tokenizations remain unverified.
Morphology cannot resolve every ambiguous compound meaning; general-POS merged
compounds are omitted to avoid treating movement past something as excess.
