# 〜である

Status: [~]

## Meaning

Formal copula.

## Verified coverage

Matches split copular で + ある, であった and であれば after an
adjacent expression, including attributive である. The span reserves the
copula while retaining the preceding expression. Focused Sudachi tests cover
these forms, boundaries and controls; integration tests cover reservation,
identity and overlap with explanatory 〜んだ in のである.

Sudachi can analyze locative expressions such as 机の上である and
学校である会議 as the same copula, so these ambiguous readings also match.
Other inflections, spellings and tokenizations remain unverified; independent
precision and recall evaluation remains.
