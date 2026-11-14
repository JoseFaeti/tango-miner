# 〜ようものなら

Status: [~]

## Meaning

If one were to do something, typically with serious or undesirable consequences.

## Tracked scope

The rule matches a volitional verb followed immediately by nominal `もの` and
conditional `なら`, with a following clause. Its span includes the whole verb,
because Sudachi treats the volitional form as one token. Tests cover godan,
ichidan, する and 来る forms, spacing, incomplete sentences, `物`, and the
separate potential `ものなら` construction. The rule does not reserve tokens;
the general volitional and `なら` cards can also match.

The matcher cannot verify whether the consequence is undesirable or whether a
following token constitutes a complete clause. Other tokenizations and
colloquial variants have not been assessed; corpus precision and recall have
not been evaluated.

## Implementation

- Rule and tests: [youmononara](../../../src/grammar_rules/rules/youmononara/)
- Registry identity: `youmononara`; stable identity: `〜ようものなら`.
