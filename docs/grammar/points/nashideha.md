# 〜なしでは

- [Meaning and supported form](#meaning-and-supported-form)
- [Verification](#verification)
- [Limitations](#limitations)

- [Shared implementation and evidence](nashini-nashideha.md#meaning-and-coverage)

Status: [x]

## Meaning and supported form

Noun or pronoun + なしでは means “without X” or “without X, ...”. The matcher
also accepts the spelling 無しでは. The antecedent, なし, で, and は must be
adjacent; an explicit antecedent is required.

This form is recognized by the shared `nashini_nashideha` rule, whose registered
expression also includes 〜なしに. It reserves only なしでは and retains the
antecedent as a separate vocabulary candidate.

## Verification

Sudachi full dictionary regressions cover 努力なしでは成功しない,
彼の助けなしでは無理だ, direct pronoun attachment (彼なしでは), and 無しでは.
Controls cover なしでは without an overt antecedent and a gap in で は. A
pipeline test checks token reservation and the shared grammar card identity.

## Limitations

The matcher identifies the local form and does not interpret the following
clause or resolve every lexical ambiguity. Elliptical uses without an overt
antecedent and the colloquial variant なしじゃ are outside this rule.
