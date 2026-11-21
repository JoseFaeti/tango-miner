# 〜にしてもなお

[Contents](#meaning) · [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Even so; even then, still.

## Verified coverage

Matches adjacent `にしてもなお` and `にしても尚` after a preceding expression,
using the existing にしても token analysis. Tests cover Sudachi tokenization,
boundaries, reservation, overlapping にしても and card identity. The preceding
expression remains available for mining.

Literal にする followed by てもなお can share this analysis; the matcher does
not resolve the meaning. Other tokenizations and separated forms are not covered.
