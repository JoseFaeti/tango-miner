# Sentence normalization findings

This is a code review of `src/steps/NormalizeSentences.py` and its surrounding
pipeline, not a corpus measurement or an implementation change. Normalization
runs after sentence extraction and before tokenization and grammar analysis;
therefore lost or altered text can affect candidates as well as examples. The
examples below illustrate rule behavior and have not been measured in a corpus.

## Findings

1. **Short Japanese is discarded.** The step requires at least five characters
   and two hiragana. Valid dialogue such as `えっ？`, `はい。`, and `危ない！`,
   and names such as `東京タワー！`, can be lost before mining. Short fragments
   are not always good examples, but should not automatically be excluded from
   candidate evidence.
2. **The control-code pattern also matches ordinary text.** `[A-F0-9]{2}\s`
   removes any two hexadecimal-looking characters followed by whitespace. For
   example, it removes `A4 ` in `彼は A4 用紙を使った。` and `FF ` in
   `今日は FF を遊んだ。`. It can also match inside a longer sequence. Identify
   control codes using a documented source format and boundaries, not this
   generic pattern.
3. **All whitespace is removed.** Spaces may separate Latin words, names,
   children's writing, or visual units. Removing them can join distinct words.
   Distinguish source line wrapping from meaningful spacing while extraction
   still has access to line structure.
4. **Repetition and punctuation are rewritten.** Any character repeated four or
   more times is reduced to three, changing expressive Japanese such as
   `あははははは！` and `すごーーーーい！` as well as potentially changing
   numbers. Ellipsis variants and repeated ellipses are collapsed, `・・・`
   becomes `…`, and `…。` loses its full stop. These forms can carry style or
   structure and are not necessarily mistakes.
5. **Bracket cleanup can conceal boundary problems.** A single pair enclosing a
   sentence is removed, as are certain unmatched marks at its edges. Japanese
   quotation marks can span multiple sentences: splitting `「今日は雨だ。明日は
   晴れるかな」` can leave an opening mark on one fragment and a closing mark
   on another. Stripping these does not restore context. Matching marks merely
   by their presence also does not validate nesting. ASCII apostrophes and angle
   brackets have other uses and deserve particular caution.
6. **Symbols are removed without source context.** All `→` characters and
   certain leading symbols are deleted. An arrow may express a relationship,
   and a leading symbol may be meaningful or indicate a list or dialogue.
   Apply format-specific cleanup only where its role is established.

The preceding extractor also removes some invisible and script-formatting
characters and splits on punctuation without tracking quotation context. An
end-to-end fix must inspect that stage as well; preserving text only in the
normalizer cannot recover what extraction already removed.

## Proposed direction

- [ ] Preserve original source text and source alignment alongside minimally
  normalized text. NFC is a useful normalization, but retain the original for
  display and audit. Record transformations and exclusions.
- [ ] Reassess hard length and hiragana filters using recall-sensitive examples.
  Keep useful short utterances and names available for mining; prefer complete,
  informative examples when alternatives exist.
- [ ] Replace generic destructive cleanup with source-format-specific handling.
  Preserve meaningful spaces, symbols, repetitions and punctuation. If a
  canonical form is needed for matching, derive it separately from display text.
- [ ] Improve sentence extraction and quote-aware boundary handling; retain and
  flag unresolved fragments rather than silently removing their punctuation.
- [ ] Review example ranking and fallback behavior for completeness and context,
  including legitimate short dialogue and quoted sentences. A quality score
  cannot guarantee grammaticality or self-contained meaning; Japanese allows
  omission, fragments, dialect and informal forms.
- [ ] Add focused regressions for each correction and legitimate counterpart:
  short utterances and names, `A4` and `FF`, meaningful spaces, expressive
  repetition and ellipses, nested and multi-sentence quotations, and verified
  formatting artifacts. Assess both example quality and retained candidate
  recall before rollout.

See [pipeline quality improvements](roadmap/pipeline-specific-quality-improvements.md)
and the broader [sentence analysis plan](sentence-analysis-plan.md).
