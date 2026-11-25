# 〜に関わりなく

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Regardless of; irrespective of.

## Verified coverage

The `nikakawarinaku` rule matches adjacent に関わりなく, にかかわりなく
and に拘わりなく after an adjacent non-punctuation token, including noun
and かどうか conditions. All spellings share the 〜に関わりなく card.
The preceding condition remains lexical evidence; only the phrase is reserved.

Focused Sudachi full-dictionary tests verify noun tokenization of 関わり and
かかわり, verb tokenization of 拘わり, spelling variants, boundaries and
neighboring expressions. Grammar-stage tests verify reservation, example surfaces,
identity registration and preservation of the preceding noun.

Only the adverbial なく form is supported; 関わりがなく, 関わりない,
に関わらず and にもかかわらず are outside this rule. Literal “without
involvement” readings can share this surface and are retained for review.
Other analyzer tokenizations and contextual disambiguation remain unverified.
The separate kana backlog entry describes the same supported spelling.
