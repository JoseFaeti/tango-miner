# 〜わけにはいかない

Status: [~]

## Meaning

Cannot reasonably do or bring oneself to do something because of circumstances
or obligations.

## Implementation

Recognizes a source-adjacent finite verb followed by わけ / 訳, に or には,
and いかない / 行かない, including plain past and polite negative present and
past forms. The attached verb remains lexical; reservation is subject to
dictionary expression precedence. Focused tests cover Sudachi full dictionary
analyses, negative-predicate and lexical controls, reservation and card identity.

Negative predicates are excluded: 〜ないわけにはいかない expresses an
obligation and is a separate point. Other inflections and merged analyzer
tokens are not covered. Contextual ambiguity remains, and independent precision
and recall evaluation is outstanding.

- Rule: [index.py](../../../src/grammar_rules/rules/wakenihaikanai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/wakenihaikanai/test_index.py)
- Identity: `wakenihaikanai` / `〜わけにはいかない`
