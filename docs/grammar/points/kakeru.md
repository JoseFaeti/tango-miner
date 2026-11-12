# 〜かける

Status: [~]

## Meaning

An action started but left unfinished, or an event on the verge of happening.

## Implementation

- Rule: [kakeru](../../../src/grammar_rules/rules/kakeru/index.py)
- Tests: [kakeru tests](../../../src/grammar_rules/rules/kakeru/test_index.py)
- Registry identity: `kakeru`; stable expression: `〜かける`.

Matches a continuative verb followed by a separate かける or 掛ける token,
or a merged verb ending in those forms, in plain, past, te, negative, polite
and conditional forms. The matched span
includes the preceding verb. Tokens are not reserved, preserving lexical evidence.
Standalone かける is excluded. Lexical verbs such as 話しかける also match;
matches need review because form alone cannot determine the meaning. Sudachi
can analyze 死にかける as a noun and particle followed by かける, which is
missed. Nominal 〜かけ and other inflections and segmentations remain unsupported.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
