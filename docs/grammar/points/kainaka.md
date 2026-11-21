# 〜か否か

Status: [~]

- [Meaning](#meaning)
- [Coverage](#coverage)

## Meaning

Whether or not.

## Coverage

The rule matches contiguous `か否か` after a predicate or noun, including
negative and past forms. It reserves the expression while retaining the
preceding word. Focused Sudachi and integration tests cover these forms,
alternative questions, token boundaries, and card precedence.

The kana spelling `かいなか` remains unsupported: Sudachi can analyze it as
a surname or split it as a noun, making it ambiguous with lexical readings.
