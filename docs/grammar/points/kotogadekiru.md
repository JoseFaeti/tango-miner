# 〜ことができる

Status: [~]

## Meaning and tracked scope

Ability or possibility: can do; be able to do.

- Covered: a non-past verb followed by split `ことができる` or `事が出来る`,
  including plain present, past and negative, and polite present and past,
  positive and negative forms.
- Remaining: other tokenizations, negative preceding verbs, continuative,
  conditional and other inflections. Nominalized event readings may be ambiguous.

## Implementation

- Rule: [koto_ga_dekiru.py](../../../src/grammar_rules/rules/koto_ga_dekiru.py)
- Tests: [test_grammar_koto_ga_dekiru.py](../../../tests/test_grammar_koto_ga_dekiru.py)
- Registry identity: `koto_ga_dekiru`; stable expression: `〜ことができる`.

## Existing implementation notes

The rule requires adjacent source-aligned tokens and reserves the construction
from `こと` or `事` onward, leaving the preceding verb eligible as vocabulary.
The focused tests use Sudachi full dictionary tokenizations and include lexical
controls. Dictionary expression precedence and example eligibility still apply;
these tests are not an independent precision or recall evaluation. See
[stage and precedence](../stage-and-precedence.md) and
[identity and review](../identity-and-review.md).
