# 〜かいな / 〜やん / 〜やろ

Status: [~]

## Meaning

Dialectal endings, especially associated with Kansai: かいな expresses wondering,
やん appeals to shared recognition, and やろ expresses conjecture or confirmation.
Usage and distribution vary by region and speaker.

## Coverage and limitations

The rule recognizes adjacent か + い + な, split や + ん and the nominal
suffix token やん, and auxiliary やろ after a supported complete predicate or
nominal. It keeps the preceding word outside the grammar span. Sudachi full
SplitMode C tests cover these forms, lexical かいな, やろう and spaced controls;
integration tests cover identity, reservation and examples.

This is partial: analyzer-dependent readings such as そうやろ analyzed as the
imperative of やる are excluded to avoid claiming verb uses. Proper-noun hosts
are excluded because suffix やん can also be a name suffix. The nominal suffix
analysis remains ambiguous, and other dialectal spellings, tokenizations and
contexts are not established. The rule does not determine a speaker's dialect.
