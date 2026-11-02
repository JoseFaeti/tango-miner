# Grammar documentation

- [Next points](next.md): ordered implementation queue.
- [Implemented points](implemented.md): completed and validated scopes.
- [Later candidates](later.md): unordered candidates, grouped by existing backlog category.

Choose a point from an index and read its linked specification for its meaning,
forms, evidence and remaining work. Search `points/` for an explicitly named
point. Update that specification when behavior or evidence changes; change its
index status and move its link only when its scheduling or completeness changes.
Run `python scripts/check_grammar_docs.py` after changing the indexes or points.

The checklist states are `[ ]` not implemented, `[~]` partially implemented,
and `[x]` fully implemented and validated. Partial work stays in the queue when
scheduled. A rule's existence alone does not establish complete coverage.

For implementation, follow the [grammar skill](../../.agents/skills/grammar/SKILL.md)
and [AGENTS.md](../../AGENTS.md). Shared behavior is documented in
[stage and precedence](stage-and-precedence.md), [identity and review](identity-and-review.md),
and [inflection improvements](inflection-improvements.md).
