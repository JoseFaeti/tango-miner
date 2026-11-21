# 〜ものを

[Meaning and coverage](#meaning-and-coverage) | [Implementation](#implementation)

Status: [~]

## Meaning and coverage

Expresses concession or regret, including “although” and “if only”. The rule
recognizes ものを immediately after a finite verb, adjective or auxiliary when
followed by punctuation. This covers clause-final and punctuated concessive
uses. It cannot distinguish an omitted nominal object at a punctuation boundary;
conversely, unpunctuated uses and other tokenizations remain unsupported.

## Implementation

- [Rule](../../../src/grammar_rules/rules/monoo/index.py) and
  [focused tests](../../../src/grammar_rules/rules/monoo/test_index.py).
- Registry identity: `monoo`; the matched span is `ものを`. Sudachi full
  dictionary tokenizes the covered examples as nominal もの plus case particle を.
- Matching reserves the span for grammar; nominal objects followed directly by
  a verb remain lexical.
