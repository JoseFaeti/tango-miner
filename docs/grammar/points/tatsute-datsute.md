# 〜たって / 〜だって

Status: [~]

## Meaning

Colloquial concession: “even if; even though”.

## Implementation

The rule recognizes Sudachi's merged 接続助詞 たって after a continuative
verb, adjective or auxiliary, and split past auxiliary だ + って after a
continuative predicate, when a following clause is present. The predicate
remains available as vocabulary. Immediate speech-verb quotations are excluded.
For example, 行ったって無駄だ and 読んだって分からない are covered.

Split だって can also quote a past statement or express hearsay; the rule cannot
reliably distinguish those readings, so false positives remain. Copular and noun
uses, other tokenizations, and sentence-final concession are not covered.
Precision and recall have not been independently evaluated. Dictionary expression
precedence and grammar reservation can affect emitted candidates and frequencies.

- Rule: [index.py](../../../src/grammar_rules/rules/tatsute_datsute/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tatsute_datsute/test_index.py)
- Registry identity: `tatsute_datsute`
