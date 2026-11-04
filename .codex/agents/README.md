# Project agent roles

These project-scoped Codex roles support focused delegation. Ask for a role by
name when its contribution is useful, for example: "Have pipeline_analyzer
inspect this completed run, then have reviewer review the proposed change."
Codex coordinates the agents; no Trello integration or autonomous dispatcher is
configured. Use the smallest useful set of roles and keep handoffs to the task,
acceptance criteria, relevant paths, and evidence. The parent integrates results,
checks the final diff, and handles commits.

| Role | Focus | Intended changes |
| --- | --- | --- |
| `coder` | Features and fixes | Production code and focused tests |
| `tester` | Validation and regressions | Tests only |
| `docs` | Documentation | Documentation only |
| `refactorer` | Behavior-preserving structure | Production code and tests |
| `commit_analyzer` | Commit and diff analysis | None |
| `pipeline_analyzer` | Completed runs and quality evidence | None |
| `grammar_analyst` | Grammar and linguistic evidence | None |
| `planner` | Complex task scope and acceptance criteria | None |
| `reviewer` | Independent final review | None |

For a grammar change, a useful sequence is grammar analysis, implementation,
testing, documentation, and review. For a run investigation, begin with the
pipeline analyzer and only add implementation or testing if warranted. These
are options, not mandatory stages; simple tasks may not need delegation.
Existing skills supply domain workflows and repository instructions apply to
every role. A dedicated repair role is deferred: prefer fixing test failures in
the implementing agent's context. The pipeline analyzer can propose regression
examples without modifying the corpus.

Role boundaries are instructions, not enforced file-level permissions. Codex
subagents inherit the parent session's sandbox and approval settings; custom
agent files cannot impose their own read-only sandbox. Use a suitable parent
permission mode when enforcement matters, and review changes before integrating.
Agents inherit model and reasoning settings unless configured elsewhere. See
[OpenAI's subagent documentation](https://learn.chatgpt.com/docs/agent-configuration/subagents).
