# 〜をものともせずに

- [Meaning](#meaning)
- [Implementation and verification](#implementation-and-verification)

Status: [x]

## Meaning

Undeterred by; in spite of an obstacle.

## Implementation and verification

This variant shares the canonical [〜をものともせず](omonotomosezu.md)
card and rule identity `omonotomosezu`; it does not create a duplicate card.
The existing matcher covers をものともせずに and を物ともせずに,
including the final particle に in the matched and reserved span. The preceding
obstacle remains outside that span and available for lexical mining.

Sudachi full dictionary regressions in
[test_index.py](../../../src/grammar_rules/rules/omonotomosezu/test_index.py)
verify both spellings, noun-phrase and name attachments, punctuation, whitespace,
missing attachments, and unrelated expressions.
Grammar-stage tests verify a shared card, source surface examples, and reservation
of the complete suffix while preserving the obstacle token.

The matcher recognizes token structure rather than deciding whether the obstacle
is semantically plausible. Other inflections and analyzer segmentations remain
outside this point's scope; see the canonical specification for shared limits.
