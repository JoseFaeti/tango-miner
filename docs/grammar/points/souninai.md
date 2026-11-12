# 〜そうにない

Status: [~]

## Meaning

Unlikely to happen; shows no sign of happening.

## Implementation

The rule recognizes a continuative verb followed by split そうにない, そうになかった,
そうになくて, or そうになければ. The attached verb remains lexical; the
suffix is reserved subject to dictionary expression precedence. A following
です is outside the matched span. Standalone そう and adjective attachment
are excluded. Other inflections and merged analyses are not yet supported;
contextual ambiguity and independent precision and recall evaluation remain.

- Rule: [index.py](../../../src/grammar_rules/rules/sou_ni_nai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/sou_ni_nai/test_index.py)
- Identity: `sou_ni_nai` / `〜そうにない`
