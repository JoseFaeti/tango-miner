# 〜ては / 〜ちゃ / 〜じゃ

Status: [~]

## Meaning and implementation

Conditional or repeated action: if or whenever an action occurs, or doing it
repeatedly. The rule recognizes verbal `ては`, `では`, `ちゃ`, and `じゃ`
after an adjacent continuative verb, including split `て` / `で` plus `は`.
It reserves the suffix while retaining the verb. These forms can overlap with
prohibition and other grammar rules; the matcher does not distinguish meanings
from context. Copular `じゃ` and the distinct `ちゃう` / `じゃう` auxiliary
are excluded. Adjective and negative predicates, merged verb-plus-suffix tokens,
and broader contextual evaluation remain outstanding. Precision and recall have
not been measured on a corpus.

- Rule and tests: [implementation](../../../src/grammar_rules/rules/teha_chiya_jiya/index.py), [tests](../../../src/grammar_rules/rules/teha_chiya_jiya/test_index.py).
- Registry identity: `teha_chiya_jiya`; expression: `〜ては / 〜ちゃ / 〜じゃ`.
- Shared boundaries and precedence: [stage and precedence](../stage-and-precedence.md).
