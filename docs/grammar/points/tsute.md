# 〜って

Status: [~]

## Meaning

Quotation, topic or hearsay.

## Verified coverage

Matches an adjacent Sudachi particle `って` after a preceding token, including
topic (猫ってかわいい), quotation (明日行くって聞いた), sentence-final
hearsay (雨だって), and quotation after a closing bracket. Only the particle
is reserved, leaving the surrounding words available as lexical evidence.
The registered attributive 〜っていう rule takes precedence when its full
construction matches. Dictionary-expression precedence also applies.

Focused tests cover analyzer tokenization, te-form verbs, spacing, overlap,
identity, reservation and lexical components. Sentence-initial だって is
excluded as a common conjunction, and punctuation other than a closing bracket
is not supported before the particle. The particle is ambiguous: for example,
猫だって can mean “even cats,” and such uses can also match. Quotation after
other punctuation, standalone or spaced particles, and other tokenizations
remain unsupported; the rule does not distinguish its individual senses.
