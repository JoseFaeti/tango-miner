# 〜た

Status: [~]

## Meaning and tracked scope

Past or completed event.

- Covered: source-aligned terminal and attributive た / だ auxiliaries after
  continuative verbs, adjectives and auxiliaries, including negative, polite
  and copular predicates.
- Remaining: other tokenizations and non-past uses of this morphology;
  independent precision and recall evaluation.

## Implementation

- Rule: [ta/index.py](../../../src/grammar_rules/rules/ta/index.py)
- Tests: [ta/test_index.py](../../../src/grammar_rules/rules/ta/test_index.py)
- Registry identity: `ta`

The rule retains the auxiliary as the source span and can overlap other grammar
cards, such as 〜たことがある and だった / でした. It excludes hypothetical
たら and listing たり by requiring a terminal or attributive inflection.
Morphology does not establish whether every occurrence expresses past or
completion; for example, some た forms have other discourse functions. Merged
tokens and analyses lacking the required inflection are not covered.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
