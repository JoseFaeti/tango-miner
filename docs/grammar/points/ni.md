# に

Status: [~]

## Meaning

Time, destination, location and recipient.

## Verified coverage

Matches a complete Sudachi `に` case particle (`助詞, 格助詞`) adjacent to
a preceding token, including time, destination, location and recipient uses.
Only the particle is reserved; neighboring words remain lexical evidence.
Dictionary-expression precedence applies, and longer grammar rules can overlap
it, as in `ことになる`.
The case particle can also be matched within degree expressions such as
`ここまでに美しい`; this does not classify the expression as a deadline.
The lexical expression `眠りにつく` takes precedence when expression
precedence is enabled, including its supported inflections.

Focused tests cover analyzer forms, lexical and boundary controls, reservation,
overlap, examples and identity. The rule does not distinguish senses and can
also match other case-particle uses. Standalone and spaced particles,
punctuation other than a closing bracket before the particle, and forms
tokenized within a larger token are unsupported. Corpus precision, recall and
effects on lexical candidates have not been measured.
