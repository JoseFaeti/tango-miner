# Purpose

[Contents](../pipeline-improvement-roadmap.md)


This document records the most valuable next improvements to the mining
pipeline.  It is based on the current executable path in `tango_miner.py`:

```text
discover files -> read -> extract sentences -> normalize -> raw Sudachi tokens
-> JMdict compound merge -> build word statistics -> frequency filter
-> definitions -> score -> attach examples -> CSV -> AnkiConnect
```

The recommendations preserve the project's current **recall-first** policy.
Diagnostics should make a questionable candidate explainable; they should not
silently add broad exclusion rules that hide grammar, colloquialisms, or
source-specific vocabulary.

