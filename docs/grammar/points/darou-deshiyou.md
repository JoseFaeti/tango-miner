# だろう / でしょう

Status: [~]

## Meaning

Conjecture or confirmation: probably, I suppose, or a request for agreement.

## Implemented

Matches complete Sudachi auxiliary tokens だろう, でしょう and colloquial
でしょ, including standalone responses and occurrences after predicates, nouns,
and explanatory ん. The matched span is the auxiliary alone; an explanatory
んだ card may also match the larger phrase. The rule checks both lemma and part
of speech to avoid treating a lexical token as this ending.

Focused tests cover actual Sudachi analyses, questions, standalone responses,
lexical and copula controls, token boundaries, and card integration.

## Limitations

The same form can express conjecture or seek confirmation; the rule does not
classify meaning. Other spellings and tokenizations remain unverified. The
separate のだろうか point is not implemented by this rule.
