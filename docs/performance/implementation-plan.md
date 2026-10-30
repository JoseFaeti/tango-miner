# Performance implementation plan

[Contents](../pipeline-performance-plan.md)

Status: `[ ]` not started, `[~]` partially complete, `[x]` completed to the
stated scope and validated. Partial items identify the remaining work.

- [~] Audit archive fields and measurement overhead.
  - Covered: existing stage, cache, configuration and dependency records were
    inspected and the named timing schema was introduced.
  - Remaining: quantify profiling overhead on a representative bounded sample.
- [x] Add optional per-step totals and named phase timings, with grammar as the
  pilot. Record cache hits and partial timings on failures; preserve matching
  behavior. Focused tests cover cache hits, interrupted key preparation, and
  unchanged grammar occurrences and spans.
- [~] Provide a reusable report combining performance, stage and cache records.
  - Covered: `scripts/report_performance.py` handles current and earlier
    schemas, missing records and basic run comparisons.
  - Remaining: test mixed, malformed and incomplete archives more thoroughly
    and support summaries of variation across repeated measurements.
- [~] Investigate grammar expression-precedence cost.
  - Covered: measured the main phases, reused the dictionary within an
    invocation, and added precedence subphase timings.
  - Remaining: assess reuse or per-source caching of merge evidence with
    representative data; verify timing and source-supported behavior.
- [ ] Measure and improve repeated grammar rule scans while preserving all
  supported tokenizations and overlapping expressions.
- [ ] Measure and improve grammar cache-key preparation for warm and changed
  inputs, preserving all required invalidation dependencies.
- [ ] Compare instrumented and uninstrumented behavior and timings on a bounded
  representative fixture. Full pipeline runs require a corpus agreed with the
  user and Anki disabled or output-only.
- [ ] Add phase measurements to other steps when a concrete performance
  question warrants them; avoid collecting per-function or per-sentence traces.

For implementation changes, validate candidates, spans, examples, ordering and
cache behavior as appropriate, and run the pipeline test suite. Treat missing
measurements distinctly from zero and do not sacrifice recall for speed.
