# 〜につき

- [Meaning and forms](#meaning-and-forms)
- [Implementation and verification](#implementation-and-verification)
- [Limitations](#limitations)

Status: [~]

## Meaning and forms

Formal noun + につき / に付き expresses cause (雨天につき中止),
distribution (一人につき二枚), or a topic (事件につき説明する).
The stable card is 〜につき; only the construction is reserved, retaining
the attached noun as vocabulary.

## Implementation and verification

src/grammar_rules/rules/nitsuki/ matches adjacent noun, pronoun, or
noun-suffix + case particle に + verb つき / 付き.
Sudachi full split-mode C was checked on the examples above and on
工事中に付き通行止め. Clear auxiliary, verb, て, つつ, and ながら
continuations are excluded to retain literal verb uses.

Focused tests cover both spellings, counters, topic usage, lexical controls,
alignment gaps, card frequency, example surface, and reservation with noun retention.
Dictionary merging leaves に + つき / 付き separate: the dedicated rule owns
this construction under the single identity 〜につき, avoiding a duplicate
lexical につき entry. Integration tests retain literal つく in 王の座につきし,
証言台につきなさい, and 味方につきます while preserving cause and distribution.
Both grammatical and literal uses can be tokenized as に + verb つく;
verb POS alone is therefore not a rejection criterion.

## Limitations

Bare continuative literal 付く can share the same tokens; semantic ambiguity
remains, so this point is partial. No corpus precision claim is made.
Other kanji spellings, merged tokens, and polite につきまして forms are
outside this matcher; the latter has a separate point.
