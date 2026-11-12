# 〜じゃないと / 〜でないと

Status: [~]

## Meaning

Unless; otherwise.

## Verified coverage

Recognizes じゃないと and でないと after nominal, pronominal and
na-adjective predicates, and standalone じゃないと as a connective. The
matched span is the suffix. Sudachi-based tests cover these forms, quotation,
verbal negatives and lexical retention; an integration test checks reservation
and card examples.

## Limitations

The analyzer's distinction between conditional and quotative と is used as
evidence, but context can be ambiguous. Other tokenizations, standalone
でないと and predicate types beyond those tested need investigation. Existing
shorter grammar rules may also report overlapping spans.
