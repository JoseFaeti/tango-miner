# 〜とされている

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Remaining limitations](#remaining-limitations)
- [Implementation](#implementation)

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

The shared [base rule](tosareru.md#verified-coverage) excludes lexical
`必要とされている` while retaining `問題とされている` and
`必要だとされている`.

The reported source sentence, 禁断の古代呪法のために、たくさんの魂を必要とされている。,
is covered by analyzer and pipeline regressions for both judgment cards.
The pipeline check preserves 必要とする and its original components.
The two judgment card identities remain separate for compatibility.

## Remaining limitations

Morphology cannot distinguish every judgment from other readings. Coverage
depends on the component rules and their tokenization; contracted and other
inflections have not been independently validated for this construction.
Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [tosareteiru](../../../src/grammar_rules/rules/tosareteiru/)
- Registry identity: `tosareteiru`; stable identity: `〜とされている`.
