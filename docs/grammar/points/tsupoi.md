# 〜っぽい

Status: [~]

## Meaning

Resemblance or tendency.

## Implementation

- Rule: [tsupoi](../../../src/grammar_rules/rules/tsupoi/index.py)
- Tests: [tsupoi tests](../../../src/grammar_rules/rules/tsupoi/test_index.py)
- Registry identity: `tsupoi`; stable expression: `〜っぽい`.

Matches a separate adjective-like `っぽい` suffix after a noun or continuative
verb, including plain, past, negative, te, adverbial and conditional forms,
and optional `です`. The span starts at the suffix, leaving the attached word
available; normal dictionary-expression precedence and reservation apply. Tests
use Sudachi full dictionary forms and check lexical controls and reservation.

Sudachi can merge lexicalized forms such as `安っぽい` into a single adjective;
these are not matched, preserving the lexical reading. Standalone `っぽい` and
other segmentations or attachments remain unsupported. Morphology alone cannot
resolve the meaning of every matched occurrence; corpus precision and recall
have not been measured. See [stage and precedence](../stage-and-precedence.md).
