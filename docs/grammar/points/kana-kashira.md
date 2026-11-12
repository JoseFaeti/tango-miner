# 〜かな / 〜かしら

Status: [~]

## Meaning

Wondering.

## Verified behavior

Matches Sudachi sentence-final particles か + な / なあ and かしら after
adjacent verbs, adjectives, auxiliaries, nouns, pronouns or adverbs. The card
span contains the particles only; examples include 行くかな、雨かしら and
どうかなと思う. Quoted or embedded wondering is included. The particle tokens
are reserved while the attached word remains available as a lexical candidate.
Tests cover these forms, lexical かな and
かなしい, and token boundaries.

## Limitations

The matcher depends on Sudachi's particle analysis and does not determine
whether a question expresses genuine wondering, a suggestion, or a wish.
Other tokenizations and elongated spellings have not been validated. The
separate 〜ないかな point remains unimplemented.
