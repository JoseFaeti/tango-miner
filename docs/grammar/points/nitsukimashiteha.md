# 〜につきましては

- [Meaning and forms](#meaning-and-forms)
- [Implementation and verification](#implementation-and-verification)
- [Limitations](#limitations)

Status: [~]

## Meaning and forms

Noun + につきましては introduces a topic politely: “as for; regarding”.
The stable card is 〜につきましては. Only the construction is reserved;
the preceding noun, pronoun, or nominal suffix remains available as vocabulary.

## Implementation and verification

`src/grammar_rules/rules/nitsukimashiteha/` matches adjacent に + つき +
まし + ては, also accepting separate て + は tokens. Sudachi full split-mode C
produces the joined tail in 契約につきましては、後日説明します。 and the
split tail in それにつきましては問題ありません。 Tests cover both,
nominal suffixes, missing は, other inflections, punctuation and alignment gaps,
card identity, frequency, example surfaces and reservation with noun retention.

The existing 〜について rule also recognizes the polite prefix につきまして
when て is separate. Both cards retain their identities under the current
independent-rule aggregation; 〜につき excludes this auxiliary continuation.

## Limitations

Literal kana つく can have the same token sequence; semantic disambiguation
remains unresolved, so this point is partial. Kanji 付き and 着き are excluded
to preserve literal attachment and arrival readings. Bare につきまして and
については are outside this card. No corpus precision claim is made.
