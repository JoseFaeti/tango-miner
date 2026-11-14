# 〜かねない

Status: [~]

## Meaning

Might, undesirable outcome.

## Implementation

- Rule: [kanenai](../../../src/grammar_rules/rules/kanenai/index.py)
- Tests: [kanenai tests](../../../src/grammar_rules/rules/kanenai/test_index.py)
- Registry identity: `kanenai`; stable expression: `〜かねない`.

Matches a continuative verb followed by かねない or 兼ねない, including split
suffixes and merged verb compounds. Supports plain, past, continuative, and
conditional negative forms. The span includes the verb; tokens are not reserved
by this rule, retaining lexical evidence. Other overlapping rules can reserve
tokens independently. Standalone 兼ねない and positive かねる are excluded.
Compound spelling cannot establish whether the outcome is undesirable; ambiguous
uses require review. Other inflections and analyzer segmentations are unsupported.
