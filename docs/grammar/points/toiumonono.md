# 〜というものの

- [Meaning and scope](#meaning-and-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning and scope

Concessive “although; even though”: a statement is acknowledged before a
contrasting qualification. The rule recognizes adjacent `というものの` and
`と言うものの` after a preceding token, followed by a comma and text in the
same sentence. Sudachi's full dictionary splits `もの` and `の`; the comma
conservatively distinguishes nominal uses such as `というものの名前`.

Unpunctuated uses and other tokenizations remain unsupported. A comma and
following text do not guarantee a concessive meaning. Matched tokens are
reserved from lexical processing; preceding evidence remains available.

## Implementation

- Rule: [toiumonono](../../../src/grammar_rules/rules/toiumonono/index.py)
- Tests: [focused regressions](../../../src/grammar_rules/rules/toiumonono/test_index.py)
- Registry identity: `toiumonono`
