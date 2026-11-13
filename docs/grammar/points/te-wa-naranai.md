# 〜てはならない

Status: [~]

## Meaning and implementation

Prohibition: an adjacent verb continuative followed by `ては`, `では`,
`ちゃ`, or `じゃ` and selected complete forms of `ならない`. The rule
recognizes plain and polite present and past forms, as well as `ならぬ` and
`ならん`. It retains the verb and can overlap the general
[〜ては](teha-chiya-jiya.md) rule. Other inflections, tokenizations and
contextual ambiguity remain to be evaluated; corpus precision and recall have
not been measured.

- Rule and tests: [implementation](../../../src/grammar_rules/rules/te_wa_ikenai/index.py), [tests](../../../src/grammar_rules/rules/te_wa_ikenai/test_index.py).
- Registry identity: `te_wa_naranai`; expression: `〜てはならない`.
- Shared boundaries and precedence: [stage and precedence](../stage-and-precedence.md).
