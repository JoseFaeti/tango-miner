# 〜といわず〜といわず

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Both examples and, by implication, everything in the range; without distinction.

## Implementation

Matches two noun- or pronoun-attached といわず / と言わず markers in one
sentence, including a comma between examples. The first marker supplies the
example span, leaving the second example available for vocabulary mining.
Tests cover spelling, repeated pairs, boundaries, lexical controls, reservation
and registry identity using Sudachi full-dictionary tokenization.

The matcher does not establish the semantic relationship between the examples;
unrelated repeated literal uses may match. Other tokenizations and non-nominal
examples remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/toiwazutoiwazu/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/toiwazutoiwazu/test_index.py)
- Registry identity: `toiwazutoiwazu`; stable expression: `〜といわず〜といわず`.
