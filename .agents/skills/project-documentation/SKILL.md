---
name: project-documentation
description: Create, reorganize, or update Tango Miner project documentation and implementation plans using the repository's structure and progress conventions.
---

# Tango Miner project documentation

Follow AGENTS.md and the user's instructions. Inspect the relevant existing docs,
implementation and tests before writing; actual code and tests take precedence
where a document is stale. Preserve existing changes, line endings, and useful
historical context. Do not include absolute local paths or personal details.

For a multi-topic subject, follow the existing project pattern: keep a concise
entry point in `docs/`, put substantial topic pages in a dedicated `docs/`
subdirectory, and link in both directions. Reuse the existing structure for
small edits rather than splitting files unnecessarily. Check relative links and
update navigation when moving or adding pages.

For implementation plans and progress trackers, use the existing grammar tracker
convention: `- [ ]` for not started, `- [~]` for partially complete, and `- [x]`
for complete. Give each actionable item an accurate status based on available
code, tests and evidence; explain remaining work when partial and do not infer
completion merely from a plan or a single measurement. Keep objectives,
findings, completed work and proposed work distinguishable. Update affected
status markers when implementation changes their status.

Before finishing, check the diff for accidental content or newline changes and
check local links. Document measured results and their limits without presenting
uncontrolled comparisons as proven improvements.
