# 〜そうもない

Status: [~]

## Meaning

Unlikely to happen; shows no sign of happening.

## Implementation

The rule recognizes a continuative verb followed by split そうもない, そうもなかった,
そうもなくて, or そうもなければ. The attached verb remains lexical; the
suffix is reserved subject to dictionary expression precedence. A following
です is outside the matched span. Standalone そう and adjective attachment
are excluded. Sudachi can analyze 来そう as a single unrelated verb, which is not matched.
Other inflections and merged analyses are not yet supported;
contextual ambiguity and independent precision and recall evaluation remain.

- Rule: [index.py](../../../src/grammar_rules/rules/sou_mo_nai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/sou_mo_nai/test_index.py)
- Identity: `sou_mo_nai` / `〜そうもない`
