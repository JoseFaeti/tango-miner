# ほど / くらい / ぐらい

Status: [~]

## Meaning

Degree, extent or approximate amount; くらい / ぐらい can also mark a
minimum or comparison (私くらいだ).

## Implemented

Matches source-aligned Sudachi 副助詞 tokens ほど, 程, くらい and ぐらい
following an adjacent token. Examples include 死ぬほど, 三日くらい,
これぐらい and 食べたいぐらい. The particle alone is reserved and used
as the example surface; the preceding expression remains available for mining.
The focused tests use the full Sudachi dictionary and cover lexical 暗い,
standalone 位 and 程度, whitespace, identity and reservation.

## Limitations

This is based on particle tokenization, not semantic disambiguation: degree,
approximation and other particle uses share one card. Spellings or forms that
Sudachi does not analyze as separate particles are not recognized. Broader
source evidence and alternative spellings remain to be assessed.
