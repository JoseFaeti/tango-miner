# 〜かねる

Status: [~]

## Meaning

Find it difficult to do something, or be unable or unwilling to do it, often
as a polite refusal.

## Implementation

- Rule: [kaneru](../../../src/grammar_rules/rules/kaneru/index.py)
- Tests: [kaneru tests](../../../src/grammar_rules/rules/kaneru/test_index.py)
- Registry identity: `kaneru`; stable expression: `〜かねる`.

Matches a continuative verb followed by かねる or 兼ねる, including Sudachi's
verbal suffix analysis and merged verb compounds. With dictionary precedence
enabled, a source-aligned compound listed as a JMdict verb takes precedence:
split forms are merged with their component evidence, including inflected
見かねた and 待ちかねて. Dictionary lookup is not a semantic disambiguator,
and unlisted compounds remain eligible as grammar. Supported forms are plain,
past, te, positive polite, and conditional. The span includes the preceding
verb; for a split suru verb such as 同意しかねます it starts at し. Tokens
are not reserved, preserving lexical evidence. Standalone 兼ねる is excluded.
The separate 〜かねない construction and polite negatives are excluded.
Compound spelling alone cannot distinguish difficulty from other uses of
兼ねる, so ambiguous matches need review. Other inflections and analyzer
segmentations remain unsupported.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
