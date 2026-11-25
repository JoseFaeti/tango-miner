# 〜ぬきで / 〜ぬきにして

- [Meaning and coverage](#meaning-and-coverage)
- [Limitations](#limitations)

Status: [~]

## Meaning and coverage

Without or leaving something out. The rule recognizes separate 抜き or ぬき
noun tokens followed by で or にして, and reserves the construction while
retaining preceding words. Sudachi full dictionary tests cover both spellings,
both constructions, and lexical compounds such as 手抜き.

## Limitations

The matcher cannot determine the intended sense from context; literal or
ambiguous uses may also match. Other tokenizations and related forms such as
抜きにする need separate investigation. The distinct 〜抜きでは form is
covered by [its own rule](nukideha.md).
