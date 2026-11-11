# 〜みたいに / 〜みたいな

Status: [~]

## Meaning

Resemblance.

## Implementation

- Rule: [mitai_ni](../../../src/grammar_rules/rules/mitai_ni/index.py)
- Tests: [mitai_ni tests](../../../src/grammar_rules/rules/mitai_ni/test_index.py)
- Registry identity: `mitai_ni`; stable expression: `〜みたいに / 〜みたいな`.

Matches adjacent noun, pronoun, or finite predicate followed by Sudachi's
separate `みたい` and copular `に` or `な`. The span covers only the latter
two tokens, retaining the attached word; normal dictionary-expression
precedence and reservation apply. Tests cover Sudachi forms, lexical controls,
reservation and card examples.

Other attachments and tokenizations, including standalone or discourse-marker
uses, remain unsupported. Morphology cannot reliably distinguish resemblance
from inference or approximative uses; corpus precision and recall have not
been measured. Finite `みたいだ` is a separate point. See
[stage and precedence](../stage-and-precedence.md).
