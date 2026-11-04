# 〜ものの

Status: [~]

## Meaning and tracked scope

concession.

  - Covered: a single conjunction token after a supported predicate.
  - Remaining: other valid tokenizations and contexts; distinguish nominal
    もの + の and evaluate whether the surrounding context is concessive.

## Implementation

- Rule: [src/grammar_rules/rules/monono/index.py](../../../src/grammar_rules/rules/monono/index.py)
- Tests: [tests/test_grammar_monono.py](../../../src/grammar_rules/rules/monono/test_index.py)
- Registry identity: `concessive_monono`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ものの` card covers concessive “although ...” uses when
Sudachi identifies a single `ものの` conjunction immediately after a verb,
adjective, or auxiliary in terminal or attributive form, and text follows it.
Split nominal `もの` + `の` is not matched. This narrow analysis can miss
valid constructions and does not prove that the following text is a complete
contrast; matching tokens are reserved from lexical processing. Existing notes with this
identity receive the same protection as the other grammar cards.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
