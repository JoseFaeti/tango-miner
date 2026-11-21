# 〜でなくて何だろう

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Limitations](#limitations)
- [Implementation](#implementation)

Status: [~]

## Meaning

If this is not X, what else could it be? A rhetorical assertion that it is X.

## Verified coverage

Matches adjacent `でなくて何だろう` after a noun, pronoun or nominal
adjective, optionally followed by `か`. The suffix is reserved while the
preceding word remains available. Focused tests cover Sudachi segmentation,
reservation and misleading or incomplete forms.

## Limitations

Literal questions cannot be distinguished from rhetorical assertions. Sudachi
can segment a nominal plus `で` as a verb, as in `愛でなくて`, so such cases
are missed. Colloquial `じゃなくて`, other endings and segmentations, and
corpus precision and recall remain unassessed.

## Implementation

- Rule and tests: [denakutenandarou](../../../src/grammar_rules/rules/denakutenandarou/)
- Registry identity: `denakutenandarou`; stable identity: `〜でなくて何だろう`.
