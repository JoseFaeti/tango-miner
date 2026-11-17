# Grammar performance

- [Current behavior](#current-behavior)
- [Measurements](#measurements)
- [Next investigation](#next-investigation)
- [Measurement safeguards](#measurement-safeguards)

[Grammar documentation](README.md) · [Pipeline performance](../pipeline-performance-plan.md) ·
[Measurement evidence](performance-evidence.md)

## Current behavior

Grammar matching uses a conservative surface index for nine selective rules.
Unindexed rules and custom replacements run unconditionally. Candidates retain
registration order; matchers and expression precedence decide the final spans.
When an indexed matcher changes, recheck every trigger against its supported
inflections, contractions and analyzer boundaries. See
`src/grammar_rules/dispatch.py` for the current trigger list.

The dispatch implementation was compared with the original path for rule IDs,
spans, occurrences, examples, order and lexical reservation. Focused tests also
cover positive forms, boundaries, expression precedence and parallel execution.
Changing the shared parallel worker module invalidated both grammar and merge
caches once; subsequent runs can reuse entries created with the new fingerprint.

## Measurements

The latest completed run used four workers and the documented JMdict baseline.
Grammar took 214.8 seconds on a cache miss: 17.4 seconds for the key, 195.4 for
parallel work and 1.5 for the cache write. Merge took 51.6 seconds with 89
cache misses. The preceding run took 25.7 and 38.6 seconds respectively with
both caches warm. Its inputs, configuration and dependencies matched, but its
revision had 355 grammar rules versus 396 in the newer run.

These runs do not isolate dispatch performance. A paired single-process test on
2,853 selected examples took 6.22–6.35 seconds with the original path and
5.56–5.76 seconds with the index, preserving the compared output. This bounded
sample does not establish a corpus-wide speedup or recall on unseen input.
Details and earlier measurements are in [measurement evidence](performance-evidence.md).

## Next investigation

The newest cache-miss run was slower than an older miss, but the runs have
different rule counts and code; the parallel timing combines matching,
expression precedence, alignment and worker overhead. Prioritize a controlled
comparison with identical input, revision, workers and cache state that separates
those costs. On warm hits, separately profile cache-key preparation. Reassess
before expanding dispatch or changing worker configuration.

## Measurement safeguards

Use `scripts/report_performance.py` for completed-run timings,
`scripts/profile_grammar_rules.py` to rank rules on bounded archived inputs,
and `scripts/benchmark_grammar_dispatch.py` for paired path comparisons. Keep
inputs, configuration, dependencies, cache state and machine conditions
comparable. Retain overlapping matches, source evidence and expression precedence.
Do not infer a corpus-wide gain from a selected example sample.

Follow [development guidance](../usage/development.md) for measurements and
the [agent run procedure](../usage/running.md) for any authorized full corpus
run. Do not start such a run without explicit authorization or mutate Anki.
