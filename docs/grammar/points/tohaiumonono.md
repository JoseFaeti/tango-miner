# 〜とはいうものの

- [Meaning and scope](#meaning-and-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning and scope

Concessive “although; nevertheless”: a statement is acknowledged before a
contrasting qualification. The rule recognizes adjacent `とはいうものの` and
`とは言うものの`, followed by a comma and text in the same sentence. It also
accepts sentence-initial use. Sudachi's full dictionary analyzes `もの` and `の`
as separate tokens; the comma provides a conservative boundary against nominal
uses such as `とはいうものの名前`.

Valid unpunctuated uses and other tokenizations remain unsupported. A comma and
following text do not prove a concessive meaning, so ambiguous cases remain
possible. The matched tokens are reserved from lexical processing.

## Implementation

- Rule: [tohaiumonono](../../../src/grammar_rules/rules/tohaiumonono/index.py)
- Tests: [focused regressions](../../../src/grammar_rules/rules/tohaiumonono/test_index.py)
- Registry identity: `tohaiumonono`
