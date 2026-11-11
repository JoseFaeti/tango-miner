# 〜られる / 〜れる

Status: [~]

## Meaning

Ability, including colloquial potential.

## Supported behavior

An adjacent verb in the irrealis form followed by a られる / れる auxiliary
is matched at the auxiliary token. Inflected auxiliary stems are included;
tense and negative suffixes remain outside the highlight. The verb remains
available as lexical evidence. Focused analyzer tests cover 食べられる,
見られた, 行かれる and 来られない, as well as causative-passive and lexical
controls. Dictionary-expression precedence and example eligibility apply.

## Limitations

These forms can also express passive, honorific or spontaneous meanings;
the rule cannot distinguish them and the card identifies that ambiguity.
Sudachi analyzes forms such as 読める and colloquial 食べれる as whole verbs;
they are not split or matched. Causative-passive auxiliaries are handled by
the separate 〜させられる rule. Other tokenizations, inflections and corpus
precision and recall remain unassessed.
