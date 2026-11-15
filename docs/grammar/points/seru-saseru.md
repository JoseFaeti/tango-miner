# 〜せる / 〜させる

Status: [~]

## Meaning

Causative: make or let someone do something, depending on context.

## Supported behavior

An adjacent verb and Sudachi causative auxiliary せる / させる produce a card.
The highlighted and reserved span is the auxiliary itself, including its
inflected stem but excluding the verb and subsequent tense or negative tokens.
Tests cover godan, ichidan, する and 来る, past, negative, polite and conditional
forms, lexical controls, token boundaries and the causative-passive interaction.
For 来させます Sudachi splits 来さ + せ, so the span is せ. The lexical verb
remains available. Causative-passive is handled by the separate
[〜させられる](saserareru.md) rule. Dictionary-expression precedence and
eligible-example requirements apply.

## Limitations

The rule does not distinguish permission from compulsion. Other tokenizations,
inflections, colloquial forms and corpus precision and recall remain unassessed.
