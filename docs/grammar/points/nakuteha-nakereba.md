# 〜なくては / 〜なければ

Status: [~]

## Meaning and implementation

Elliptical obligation: must or have to. The rule recognizes verbal `なくては`
and `なければ` at an utterance boundary, optionally followed by final particles.
It reserves the suffix while retaining the verb. Complete obligations belong to
the [complete obligation rule](obligation.md); non-final conditionals remain
available to other rules. Sentence-final conditionals can still be ambiguous,
and merged verb-plus-suffix tokens and other contextual forms are not covered.
Independent precision and recall evaluation remains needed.

- Rule and tests: [nakuteha_nakereba](../../../src/grammar_rules/rules/nakuteha_nakereba/index.py), [tests](../../../src/grammar_rules/rules/nakuteha_nakereba/test_index.py).
- Registry identity: `nakuteha_nakereba`; expression: `〜なくては / 〜なければ`.
- Shared boundaries and precedence: [stage and precedence](../stage-and-precedence.md).
