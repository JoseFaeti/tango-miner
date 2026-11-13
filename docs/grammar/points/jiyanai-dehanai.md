# じゃない / ではない

Status: [~]

## Meaning

Negative copula.

## Verified behavior

- Matches じゃない and ではない after adjacent nouns, pronouns and
  adjectival nouns, including なかった, ありません, ありませんでした and
  plain forms followed by です. Only the negative copula is reserved.
- Focused tests cover analyzer tokenization, predicate and whitespace controls,
  incomplete endings, reservation, examples and identity registration.
- In the reported ところではない and ところではありません sentences,
  negative copula recognition takes precedence over the partial dictionary
  expression ところでは; the preceding ところ remains available as a word.
- Other forms including でない, continuatives, conditionals, colloquial
  negatives and merged tokenizations remain unsupported. Questions and longer
  constructions can also receive this card; overlap is not disambiguated.
  Precision and recall have not been independently evaluated.
