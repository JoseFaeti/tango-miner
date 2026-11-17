# Grammar performance evidence

- [Run observations](#run-observations)
- [Indexed dispatch comparison](#indexed-dispatch-comparison)
- [Earlier bounded measurements](#earlier-bounded-measurements)

[Grammar performance](performance.md) · [Grammar documentation](README.md)

## Run observations

These elapsed times are observations of different runs, not controlled
speedup estimates. The newer runs used the same input manifest, configuration,
dependencies and four workers, but different revisions and cache states.

| Run | Rules | Grammar cache | Grammar | Key | Parallel work | Merge cache | Merge |
| --- | ---: | --- | ---: | ---: | ---: | --- | ---: |
| November 13 | 185 | miss | 131.8 s | 17.3 s | 113.2 s | 89 hits | 31.6 s |
| November 16 | 355 | hit | 25.7 s | 18.9 s | not executed | 89 hits | 38.6 s |
| November 17 | 396 | miss | 214.8 s | 17.4 s | 195.4 s | 89 misses | 51.6 s |

Parallel work combines matching, precedence, alignment and worker overhead;
the archives do not separate those phases. The November 13 run processed
500,662 streams, compared with 500,229 in the later runs. The November 17
grammar cache write took 1.5 seconds. Machine load was not recorded.

## Indexed dispatch comparison

On 2,853 retokenized, deduplicated archived examples from 39 tags, capped at
75 per tag, two rule-profile passes identified `tte`, `toiu`,
`toshitemo_nishitemo`, `nikimatsuteiru` and `nikimatsuteru` as the slowest.
This is a selected example sample; tokenization and alignment are excluded from
the per-rule timings, which include result collection and checksumming.

Three alternating comparisons used one process, a warm in-process JMdict and
no grammar cache. All proposal IDs, spans and order, occurrences, examples,
expression-precedence results and reserved tokens matched. Calls decreased
from 1,129,788 to 1,105,433, or 396.00 to 387.46 candidates per sentence.

| Measurement | Original | Indexed |
| --- | ---: | ---: |
| Candidate collection | 0.0002 s | 0.0050–0.0053 s |
| Matcher pass, including proposal collection | 5.263–5.325 s | 4.745–5.037 s |
| Grammar stage, including alignment and precedence | 6.217–6.347 s | 5.559–5.763 s |

Stage timings exclude tokenization, reservation and checksumming. The sample
and single-machine timing cannot establish corpus-wide savings or recall.
Repeat the comparison with an archive's sentence CSV:

```sh
python -X utf8 scripts/benchmark_grammar_dispatch.py RUN_DIR/input.sentence.csv --limit 3000 --per-tag 75 --repeat 3
```

## Earlier bounded measurements

These historical measurements informed individual changes; samples and
conditions differ, so their savings cannot be added or generalized to a corpus.

| Area | Evidence and resulting change |
| --- | --- |
| Grammar phases | One miss took 160.2 s, including 69.9 s precedence, 52.5 s matching and 30.3 s key preparation. A later miss took 135.5 s, including 50.7 s matching, 44.1 s precedence and 33.1 s key preparation; precedence included 32.1 s merging, 8.9 s dictionary access and 1.6 s alignment. |
| Dictionary reuse | Resolving the JMdict object once per invocation avoids repeated path resolution; 10,000 warm accesses previously took 1.34 s. An invocation-local 8,192-entry best-entry cache took 0.0014 s versus 0.102 s for 12,000 synthetic repeated selections, excluding dictionary loading. |
| Cache key | On 10,000 repeated examples, direct validation loops took 0.119–0.155 s versus 0.204–0.237 s; JSON took 0.224–0.280 s and hashing 0.030–0.033 s. ASCII-escaped JSON later took 0.198–0.213 s versus 0.223–0.225 s for UTF-8 JSON, producing 16.1 MB instead of 12.3 MB. |
| Obligation matcher | Precomputing forms and prefixes reduced three passes over 10,000 repeated examples from 0.267–0.272 s to 0.031–0.036 s with identical spans. |
| `te_kuru` | On 20,000 older archived sentences, earlier lemma and connective checks took 0.041–0.047 s versus 0.103–0.194 s; all 214 spans matched. The tokenizer predates the current baseline. |
| `te_iku` | On 7,287 current-tokenizer sentences, the same ordering retained 50 spans; three passes took 0.0181–0.0491 s versus 0.0427–0.0434 s. The first updated pass was slower. |
| Merge boundaries | On 2,901 current-tokenizer streams, precomputing reserved boundaries took 0.289–0.310 s versus 0.358–0.368 s, with identical merged tokens and rejected-merge diagnostics. |
| Proposal-bearing merges | On 1,626 proposal-bearing streams from 7,287 lines, preparing surfaces and binding dictionary lookup took 0.245–0.317 s versus 0.345–0.379 s, with identical merged tokens and rejection diagnostics. |

Phase timings can overlap. In particular, dictionary, merge and alignment
subphases are included in expression precedence and must not be added to it.
