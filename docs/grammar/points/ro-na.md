# 〜ろ / 〜な

Status: [~]

## Meaning

Imperative and prohibition.

## Verified behavior

The `ro_na` rule uses the stable identity `〜ろ / 〜な`. It matches a
Sudachi-labeled imperative verb, including 食べろ, 行け and 来い, or a
dictionary-form verb immediately followed by final-particle な, as in 行くな.
The example span is the whole imperative verb or the prohibition particle.
Imperative verbs remain available for lexical mining; the separate な can be
reserved. The existing 〜な card also matches the latter. Imperative request
auxiliaries in recognized 〜てください / 〜てくれ constructions are left to
that separate card; standalone ください remains eligible. Quoted imperatives
such as 努力せいってこと retain the imperative match.

Focused tests cover both branches, lexical reservation, overlap and controls
including adjectives, copular な, かな and spaced tokens.

## Limitations

Final な after a verb can also be reflective, and imperative morphology can
occur in other requests or greetings; the rule does not determine pragmatic meaning.
It relies on Sudachi's analysis: standalone しろ！ is not identified as an
imperative in the tested dictionary. Other tokenizations, auxiliary chains and
corpus precision and recall have not been evaluated.
