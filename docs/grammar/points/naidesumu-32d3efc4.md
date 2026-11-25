# 〜ないで済む

- [Meaning](#meaning)
- [Implemented behavior](#implemented-behavior)
- [Evidence](#evidence)
- [Verification](#verification)
- [Limitations](#limitations)

Status: [x]

## Meaning

Avoid having to perform an action.

## Implemented behavior

For a verb in the negative form, the rule matches recognized ない followed by
adjacent で and 済む or すむ. Its span starts at ない, retaining the preceding
verb as lexical evidence. Supported finite endings are handled by the shared
`sumu_end` matcher.

This is the same construction as the existing 〜ないですむ point and uses its
stable `naidesumu` rule and card identity. The [alias specification](naidesumu.md)
does not add a second matcher or card. The related 〜なくて済む, 〜ずに済む,
and 〜ないでは済まない constructions have separate rules.

## Evidence

Reference material distinguishes ないで済む as avoiding an unwanted action
from ないでは済まない as an action that must be done ([Japanese grammar
reference](https://mainichi-nonbiri.com/grammar/n1-zunihasumanai/)). Whether
terminal ないで済まない without は belongs with the obligation construction
remains unverified.

## Verification

Sudachi full dictionary tests cover 行かないで済む, 行かないで済んだ,
食べないで済みました, 行かないで済めば, and 食べられないで済む. Controls
cover なくて, lexical 住む, 静かでないです, unrelated following verbs, the
terminal negative 済まない, and an ambiguous unspaced kana tokenization. A card
integration test checks reservation, identity, and retention of the verb as
lexical evidence.

## Limitations

Sudachi can analyze unspaced 行かないですんだ as ない + です + ん + だ, which
does not match this construction. The shared matcher excludes terminal 済まない;
whether no-は variants with this obligation reading should be covered remains
unverified. Other tokenizations and corpus precision and recall remain
unverified.
