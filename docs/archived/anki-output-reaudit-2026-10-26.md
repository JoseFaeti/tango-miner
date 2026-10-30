# Anki output re-audit: 2026-10-26

## Scope and method

This re-audit covers the refreshed supplied Anki export after the recall-first
filtering change and source-surface compound-merging fix.

Every output row was parsed, and every available highlighted example span was
re-tokenized with the production Sudachi configuration. As in the first audit,
this evaluates whether candidates are grounded in their examples; it does not
evaluate the English definition, which is known to require separate work.

## Comparison with the prior export

| Measure | Prior audit | Refreshed audit | Change |
| --- | ---: | ---: | ---: |
| Output entries | 17,444 | 18,855 | +1,411 |
| Entries with usable examples | 17,402 | 18,814 | +1,412 |
| Function-only raw analyses | 72 | 109 | +37 |
| Noun-plus-grammar expressions | 361 | 391 | +30 |
| Proper-noun review signals | 15 | 15 | 0 |

The increased entry count is expected and desirable under the project’s
recall-first policy. The additional function-only and noun-plus-grammar rows
are chiefly grammar or expression candidates that the previous filter would
have hidden.

## Confirmed improvements

### Incorrect lemma inventions have been removed or substantially reduced

| Candidate | Prior state | Refreshed state | Assessment |
| --- | --- | --- | --- |
| `おまつ` | 46 examples, mostly polite forms of `待つ` | Absent | Fixed: source-surface matching prevents the unrelated “black pine” target. |
| `ぬし` | 47 examples, often from `ありませんし` / `開かんし` | 8 examples | Strongly improved. The remaining examples are mostly genuine `ぬし` uses, such as `うみのぬし`. |

The targeted regression tests for both cases pass. The surface-based merge rule
therefore addresses the concrete defect without suppressing potentially useful
grammar or colloquial material.

### Grammar and colloquialisms are retained

The refreshed output now includes grammar and expression candidates such as
`んだ`, `んです`, `ないで`, `ないと`, `てく`, `ても`, `られる`, `ちゃう`,
`ですから`, `かっけえ`, `だりぃ`, and `ちげぇ`.

This matches the intended scope: the deck should help with everything useful in
the learner’s reading material, not only dictionary-style content words.

## Remaining review signals

These are not removal recommendations. They identify where a future
provenance-aware display or definition system would help.

### Productive grammar is now visible

The largest function-only candidates are `てる`, `です`, `れる`, `たの`,
`られる`, `なの`, `せる`, `ちゃう`, `ても`, and `ちまう`. These are useful
grammar, conjugation, or spoken-language forms. Their high frequency reflects
the corpus and should not be treated as a quality failure.

Some spans are less self-contained (`おも`, `おっぱ`, `おねえ`, `おとう`, and
`おかあ`), but they remain available under the recall-first policy. Their
examples can still be useful, and future work should improve their target
selection or add a confidence/provenance label rather than remove them.

### Proper-noun signals remain stable

The 15 entries with proper-noun evidence are unchanged in count. They include
some likely ordinary terms as well as game-specific terms, so the stable count
does not justify any filter. It is useful only as a future review signal.

### Definitions remain the next quality bottleneck

The top rows illustrate the expected limitation: `する`, `いる`, and `ない`
appear in valid contexts, but their selected dictionary senses can be poorly
matched to those contexts. This re-audit intentionally does not mark those
entries as bad words; it confirms that contextual definition selection is the
next quality problem after target-form correctness.

## Conclusion

The refreshed export is better aligned with the project goal than the prior
one:

- it retains more material from the reading corpus;
- it keeps grammar, expressions, and colloquialisms visible;
- it removes the demonstrated lemma-to-unrelated-word failure mode; and
- it does not introduce a new broad filtering rule that could hide useful
  material.

The current pipeline is in a good recall-first state. The next refactor should
build on existing source-token provenance to improve target presentation and
definition selection, not reduce the candidate set.

## Related documents

- [Initial output audit](anki-output-audit-2026-10-26.md)
- [Candidate-quality and recall policy](../false-positive-analysis.md)
