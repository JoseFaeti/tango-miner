# 〜得る / 〜得ない

Status: [~]

## Meaning

Possibility or impossibility.

## Implemented coverage

A source-adjacent verb in the continuative form followed by separately tokenized
得る, える or うる produces one card. Supported forms include the plain
affirmative, negative and negative past, affirmative past, polite affirmative
and negative, conditional, て and ぬ forms. The card span starts at 得る or
its kana equivalent, preserving the preceding verb as lexical evidence.
Standalone 得る and ざるを得ない do not match this rule. Sudachi full
dictionary examples and lexical controls are covered by focused tests.

## Remaining limitations

Fused tokens, other inflections and alternate tokenizations are not yet
covered. Dictionary-expression precedence may suppress a grammar occurrence;
the construction can also overlap lexical expressions such as あり得る.
Broader source evidence and recall assessment remain to be checked.
