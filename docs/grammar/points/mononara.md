# 〜ものなら

Status: [~]

## Meaning

If it were possible.

## Tracked scope

The rule matches an adjacent attributive verb followed by nominal `もの` and
conditional `なら`, provided a following clause begins. The card span is
`ものなら`. Sudachi full-dictionary tests cover potential forms and `できる`,
including a challenge, and exclude volitional `〜ようものなら`, the kanji
`物`, and incomplete sentences.

The same morphology also describes literal things, for example
`買えるものなら何でもいい` and `買うものならこれだ`; these remain
ambiguous matches because potential meaning cannot be reliably inferred from
the token alone. Tokens are not reserved by this rule, although overlapping
rules can reserve them. Colloquial or merged forms and other analyses remain
unsupported. Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [mononara](../../../src/grammar_rules/rules/mononara/)
- Registry identity: `mononara`; stable identity: `〜ものなら`.
- See [stage and precedence](../stage-and-precedence.md) and
  [identity and review](../identity-and-review.md).
