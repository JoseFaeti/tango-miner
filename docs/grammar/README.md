# Grammar documentation

- [Next points](next.md): ordered implementation queue.
- [Partially implemented points](partially-implemented.md): existing scopes with remaining work.
- [Implemented points](implemented.md): completed and validated scopes.
- [Later candidates](later.md): unordered candidates, grouped by existing backlog category.

Choose a point from an index and read its linked specification for its meaning,
forms, evidence and remaining work. Search `points/` for an explicitly named
point. Update that specification when behavior or evidence changes; change its
index status and move its link only when its scheduling or completeness changes.
Run `python scripts/check_grammar_docs.py` after changing the indexes or points.

Run `python -m tango_miner.next_grammar` to select the first unchecked entry in
`next.md`, then `later.md`, or the first partial entry in
`partially-implemented.md`. It returns UTF-8 JSON containing the `name` and
repository-relative `path`, or JSON `null` when all three are exhausted.

The checklist states are `[ ]` not implemented, `[~]` partially implemented,
and `[x]` fully implemented and validated. Move a point to the partial index
when implementation begins; a rule's existence alone does not establish complete
coverage.

Each implemented rule has `index.py` and `test_index.py` in its own folder under
`src/grammar_rules/rules/`. Shared alignment tests remain in `rules/tests/`.

For implementation, follow the [grammar skill](../../.agents/skills/grammar/SKILL.md)
and [AGENTS.md](../../AGENTS.md). Shared behavior is documented in
[stage and precedence](stage-and-precedence.md), [identity and review](identity-and-review.md),
and [inflection improvements](inflection-improvements.md).
