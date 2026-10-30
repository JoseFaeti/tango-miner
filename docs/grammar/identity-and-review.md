# Identity and review

[Grammar card index](card-details.md)

The distinct Expression is the Anki identity. A new card participates in the
existing preview and confirmation flow. If a note with that exact expression
already exists in the configured deck/model, its fields and other tags are preserved;
its frequency and examples do not refresh automatically. When the card is present
in the current output, the plan removes the old synthetic `grammar` tag unless
`grammar` is an actual input source tag. New cards receive only source tags. The plan lists it under `retained_existing_grammar`.
The identity is also protected from pruning in later runs where the stage is
disabled or no occurrence is found. With pruning enabled, a lexical note that disappears from the output can be
planned for deletion; review the entire plan before approving it. Grammar settings
do not enable Anki or disable other planned updates. Review the complete Anki plan before applying it.

Grammar identities use `〜` as an attachment marker, for example `〜ている`.
The marker does not guarantee uniqueness against vocabulary expressions; the
pipeline rejects collisions. For an existing suffix-style note, an Anki plan
renames its Expression in place when the current card exists, there is exactly
one old note, and no note already has the new identity. Other fields, custom
tags and scheduling remain intact; the existing synthetic-tag cleanup applies.
When both identities exist or multiple old notes share an identity, the plan
records `grammar_migration_conflicts` and leaves them untouched. Both old and
new identities are protected from pruning, including when grammar is disabled.
Plans still require the normal approval before any Anki mutation.
The shared grammar stage collects all registered rules in one pass; the output
card stage adds its cards before scoring. Each matcher and its metadata live in a separate module under `src/grammar_rules/rules/`;
`registry.py` lists the rules used by the shared stage. The corresponding
`tests/test_grammar_*.py` files check each rule. When adding a rule, register it
and its identity in the Anki protection set; startup validates that both
registrations match.

For a bounded proposal-review sample from an existing shadow archive:

    python -X utf8 -m src.GrammarReview --input runs/example-run/grammar-shadow.jsonl --output checks/grammar-review.jsonl --per-stratum 5

The sampler streams the archive and deterministically selects up to the limit
per construction and evidence combination. Rows include stratum population,
source text, tokens, proposal, and empty human-review fields. They are
proposal-selected and stratified, not a representative sentence sample; they
cannot measure missed grammar or corpus-wide accuracy. Their contents may be
private. Do not use the same examples for both rule design and independent
evaluation.

