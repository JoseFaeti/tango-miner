# 〜てはいけない

Status: [~]

## Meaning and tracked scope

prohibition. Covered: verb continuative + ては / では / ちゃ / じゃ + selected complete plain and polite present and past forms of いけない. Other tokenizations, inflections and contextual ambiguity remain to be evaluated.

## Implementation

- Rule: [src/grammar_rules/rules/te_wa_ikenai/index.py](../../../src/grammar_rules/rules/te_wa_ikenai/index.py)
- Tests: [tests/test_grammar_te_wa_ikenai.py](../../../src/grammar_rules/rules/te_wa_ikenai/test_index.py)
- Registry identity: `te_wa_ikenai`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).


## Supported behavior and ambiguity

A source-aligned verb continuative followed by ては / では or contracted
ちゃ / じゃ and いけない expresses prohibition. Selected complete plain
and polite present and past endings are supported; the verb remains available
for lexical processing. Other forms and tokenizations remain lexical, and
contextual ambiguity, expression precedence and corpus-level recall remain
unmeasured. The general [〜ては](teha-chiya-jiya.md) rule can also match the
suffix; [〜てはならない](te-wa-naranai.md) has a separate identity.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
