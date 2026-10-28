# Research: Japanese analysis for Tango Miner

Research date: 2026-10-28. This is a design assessment, not a benchmark of
the tools on the project's corpus. Tool performance, compatibility, licensing,
and model availability must be checked again before adoption.

## What the pipeline must distinguish

Japanese writing does not contain explicit word boundaries, and there is no
single segmentation that is always correct for every purpose. Japanese
Universal Dependencies (UD) uses NINJAL short-unit words for syntactic
annotation; a learner can additionally benefit from longer vocabulary,
expressions, conjugated forms, and grammar constructions. UniDic's maintainers
explicitly caution that its short units were designed for corpus search and
are not inherently ideal for all syntactic and semantic analyses. These are
different representations of the same text, not grounds to discard either.
[Japanese UD guidelines](https://universaldependencies.org/ja/index.html);
[NINJAL UniDic overview](https://clrd.ninjal.ac.jp/unidic/en/about_unidic_en.html).

Morphological analysis, syntactic structure, named-entity recognition,
candidate selection, and word-sense disambiguation answer different questions.
A POS tag does not prove the intended meaning, a dictionary hit does not prove
the token boundary, and a dependency parse does not by itself supply a correct
English definition. An inflected verb, a colloquial construction, and a proper
name may all be useful learning targets even if absent from JMdict.

## Evidence from this project

The current pipeline uses Sudachi split mode C, greedily merges certain
dictionary-backed surface sequences, aggregates by candidate spelling, and
selects definitions largely from JMdict and retained component evidence. It
has source-linked occurrence diagnostics, but the normal candidate identity
still combines different uses of the same spelling. Source spans in diagnostics
refer to normalized sentences, and alignment can fail. `SegmentedSentence`
stores one surface, span and component sequence per lemma per sentence, so
multiple occurrences of the same lemma within one sentence can overwrite one
another. Standalone proper nouns are currently filtered. These are implementation
facts, not claims about all Japanese analyzers.

On the locally configured Sudachi dictionary, a small targeted probe found that
all three split modes produced `こん / なにも` for `こんなにも多く...`,
`ないし` as a conjunction for `自信ないし、...`, and `ごく / もん` for
`打ち首ごくもん`. This is four example sentences, not a representative
benchmark. A/B/C are different segmentation granularities over Sudachi's
analysis, not independent analyses; agreement therefore does not establish
correctness. [Sudachi split-mode documentation](https://github.com/WorksApplications/Sudachi#the-modes-of-splitting).

The 120-row evaluation sample uses one selected example per candidate and is
balanced by output status and frequency. It is useful for a pilot but is not a
random sample of corpus occurrences. A preliminary, selective review found
errors in valid output as well as review output. See
[review evaluation](review-evaluation.md); no corpus-wide accuracy estimate
or independent gold set is available yet.

## Recommended representations

1. Preserve raw source text and a normalized view, with mappings between their
   character spans. Keep source document, sentence ID, occurrence ID, exact
   surface, offsets, and the analyzer/dictionary version. Never replace the
   quoted source with an inferred normalization.
2. Represent tokenization as multiple span-aligned proposals. Preserve Sudachi
   source form, dictionary form, normalized form, reading, detailed POS,
   conjugation, and out-of-vocabulary status. Where practical retain short and
   long unit views. Generate genuinely competing analyses from a second
   analyzer or targeted alternatives, not only A/B/C. Record which system
   proposed each analysis and do not treat agreement from shared components as
   independent confidence.
3. Represent learning candidates separately from parser tokens. A candidate
   may cover several tokens, overlap a larger candidate, represent grammar or
   a name, or associate an inflection with a lemma while retaining its source
   form. Validate boundaries against the sentence; avoid unrestricted greedy
   longest dictionary matches.
4. Attach candidate dictionary entries and senses, with their spelling,
   reading, POS, inflection and usage restrictions. Use source form and
   contextual evidence to rank possibilities, preserving uncertain or missing
   options. Keep sense IDs or a versioned sense signature: an entry can contain
   several meanings, and entry sequence alone does not establish the sense.
5. Assign occurrences to a lexical or grammar identity only after their
   analysis is supported. Retain unassigned occurrences independently, count
   examples per identity, and allow abstention. Preserve proper-noun and
   unlisted-term candidates; contextual name recognition and dictionaries are
   evidence, not proof of a reading or identity.

Japanese colloquial text adds kana spellings, omitted particles, contractions,
elongation, nonstandard forms, and fragments. In a Japanese user-generated
text benchmark, existing morphological and lexical-normalization methods
struggled particularly with non-general words and nonstandard forms; this is
evidence of a relevant domain risk, not a measured result on game scripts.
[NAACL 2021 Japanese UGT corpus](https://aclanthology.org/2021.naacl-main.438/).

## Analyzers and dictionaries to test

| Option | Useful contribution | Limits and experiment |
| --- | --- | --- |
| Sudachi | Existing fast baseline; A/B/C granularities; dictionary/normalized forms, readings and detailed POS. | A/B/C do not guarantee alternative analyses. Preserve the current C baseline; compare span-aligned A/B views without assuming either is correct. [Sudachi](https://github.com/WorksApplications/Sudachi). |
| UniDic with MeCab or another compatible analyzer | An independently configured short-unit morphological view with a different dictionary and annotation scheme. | Reconcile granularity and POS conventions before comparison. Test installation, license, speed, and actual disagreement on the corpus. [NINJAL UniDic](https://clrd.ninjal.ac.jp/unidic/en/about_unidic_en.html). |
| KWJA | Japanese foundation-model analyzer offering segmentation, normalization, readings, morphology, named entities and dependencies; CPU is supported. | Test selected modules and resources on a bounded sample. Its authors' benchmark numbers use their datasets and annotation scheme, not this game corpus; it is not automatically a JMdict sense selector. [KWJA project](https://github.com/ku-nlp/kwja); [ACL paper](https://aclanthology.org/2023.acl-demo.52/). |
| GiNZA | Japanese dependency and bunsetsu analysis with a standard and transformer-based model. | Uses Sudachi, so its segmentation is not independent of the existing tokenizer. The project documents significant memory needs for transformer models; use syntactic output as evidence and measure whether it improves decisions. [GiNZA](https://github.com/megagonlabs/ginza). |
| JMdict | Structured lexical entries, multiple readings and senses, usage tags and restrictions. | Match spelling and reading jointly and respect reading-to-kanji as well as sense restrictions. The current parser stores `stagk`/`stagr` but does not retain `re_restr` or `re_nokanji`. [JMdict documentation](https://www.edrdg.org/wiki/JMdict-EDICT_Dictionary_Project.html); [EDRDG entry/restriction documentation](https://www.edrdg.org/jmwsgi/edhelp.py?svc=jmdict). |
| JMnedict | Named-entity dictionary for people, places and other name types. | Names and readings can still be ambiguous or fictional; do not assign an identity on lookup alone. Check license and integration separately. [JMnedict documentation](https://www.edrdg.org/pub/Nihongo/enamdict_doc.html). |

EDRDG announced a parallel next-generation JMdict distribution in 2026; its
schema changes should be handled explicitly rather than assuming an old XML
parser covers both formats. Pin and hash any dictionary used for a benchmark.
[EDRDG release notice](https://www.edrdg.org/jmdict_edict_list/2026/msg00014.html).

## Contextual disambiguation

Evaluate a Japanese-capable contextual model on sentence plus exact span and
candidate analyses. Include options for another boundary, no matching
dictionary entry, multiple plausible meanings, and abstention. Ask it to
select stable IDs and evidence, then independently validate that the selected
span and ID exist. Never accept a generated definition as grounded solely
because the model returned it; compare against dictionary evidence or label it
as a generated explanation for review.

An EMNLP 2025 study found strong LLM performance on its word-sense tasks, while
a CoNLL 2024 study documents substantial failure cases. Neither establishes
performance on Japanese game dialogue or this project's JMdict inventory.
Context windows, prompting, latency, model version, privacy and cost matter;
compare local and external options only after deciding where source text may
be processed. [EMNLP 2025](https://aclanthology.org/2025.emnlp-main.1720/);
[CoNLL 2024](https://aclanthology.org/2024.conll-1.12/).

## Evaluation and rollout

- Build an independently reviewed gold set with source-aligned occurrence
  spans, acceptable segmentations, target types, readings, candidate entries
  and senses, examples, and explicit uncertain/multiple-answer cases. Include
  contexts with no dictionary entry, names, grammar, kana and colloquial text.
  Use multiple annotators or adjudication for difficult cases.
- Sample valid and review output *and raw source sentences*, including material
  excluded before candidate collection. Separate a source/game-level holdout
  from development; near-duplicate lines and same-game dialogue should not
  leak into both. Balanced error samples help diagnosis but do not establish
  population rates without appropriate weighting.
- Measure span/segmentation precision and recall, candidate recall, reading,
  POS and inflection accuracy, sense and example accuracy, name/grammar recall,
  false positives per sentence, abstention and error by confidence, plus runtime,
  peak memory and cost. Report type-level and occurrence-level results and
  uncertainty; review count and dictionary coverage are not quality metrics.
- Compare current output, span-aware alternatives, a second analyzer, and a
  contextual selector on the same held-out inputs. Inspect errors and compare
  accuracy at matched coverage; model confidence is not automatically
  calibrated. Treat the first 16 pilot judgments as preliminary and do not use
  them both to design and to independently evaluate a fix.
- First introduce a shadow analysis and explanation artifact without changing
  CSV or Anki; then implement split identities and supported examples in an
  opt-in export. Compare full-corpus outputs with Anki disabled. Design and
  test existing-note migration, duplicates, user edits and prune behavior with
  fake Anki before applying any plan.

The highest-value next experiment is a bounded, span-aligned comparison on
real game-dialogue samples, especially the `こんなにも`, `ないし`,
`ものの`, `ごくもん`, inflection, and proper-name families. This is a
testable hypothesis, not a guarantee that one analyzer or model will resolve
the queue.
