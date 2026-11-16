# 〜であれ / 〜であろうと

Status: [~]

## Meaning

Even if or whoever.

## Verified coverage

Matches adjacent copular で + あれ or で + あろう + と after an
expression, including repeated forms. It also recovers 誰, だれ, 何 and
なに followed by であれ when Sudachi analyzes で as a particle and あれ
as an interjection. The span reserves the ending and retains the preceding
expression. Focused Sudachi tests cover both forms, repetition and controls;
integration tests cover reservation, identity and overlap with 〜であろう.

The formal conjecture 〜であろう also matches within であろうと. Copular
であれ can be ambiguous with other uses of ある, and the interrogative
recovery can also include an interjection reading. Other tokenizations and
spellings, and independent precision and recall, remain unverified.
