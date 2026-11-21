# 〜とされている

Status: [~]

## Meaning

Is regarded as; is generally considered to be.

## Verified coverage

Matches the `とされる` judgment construction followed by an adjacent
`ている` form, with the span beginning at `と`. Analyzer tests cover present,
past, polite and negative forms, predicates, spacing and unrelated passives.
The preceding expression remains available for lexical processing. The
`〜とされる` and `〜ている` cards can also match the same occurrence;
dictionary expression precedence can suppress grammar matches.

## Remaining limitations

Morphology cannot distinguish every judgment from other readings. Coverage
depends on the component rules and their tokenization; contracted and other
inflections have not been independently validated for this construction.
Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [tosareteiru](../../../src/grammar_rules/rules/tosareteiru/)
- Registry identity: `tosareteiru`; stable identity: `〜とされている`.
