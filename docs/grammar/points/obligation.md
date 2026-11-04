# 〜なければならない / 〜なくてはいけない

Status: [~]

## Meaning and tracked scope

obligation. Covered: selected complete present and past, plain and polite negative endings, including なきゃ, なくちゃ and ないと. Merged verb-plus-suffix analyses and conditional past endings remain unsupported by this rule; elliptical forms are tracked separately. Independent precision and recall evaluation remains needed.

## Implementation

- Rule: [src/grammar_rules/rules/obligation.py](../../../src/grammar_rules/rules/obligation.py)
- Tests: [tests/test_grammar_obligation.py](../../../src/grammar_rules/rules/tests/test_grammar_obligation.py)
- Registry identity: `obligation`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The obligation card `〜なければならない` recognizes selected complete verbal
negative conditional forms followed by `ならない` or `いけない`, including
plain and polite present and past negative endings and `なきゃ`, `なくちゃ`, and `ないと`. It reserves
the suffix while retaining the attached verb; dictionary expression precedence
can suppress the card. Entirely merged verb-and-suffix tokens, conditional past endings and
elliptical obligations are tracked separately in [〜ないと / 〜なくちゃ / 〜なきゃ](naito-nakuchiya-nakiya.md). Surface and token boundaries do not
resolve every contextual ambiguity, and precision and recall have not been
independently evaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
