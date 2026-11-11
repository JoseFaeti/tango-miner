# 〜させられる

Status: [~]

## Meaning

Causative-passive: be made or forced to do something.

## Supported behavior

The rule matches adjacent Sudachi verb, causative auxiliary せる / させる,
and passive auxiliary られる. It highlights and reserves the two auxiliaries,
leaving the verb and following tense, negative, or other suffix available.
The passive auxiliary may be inflected, so the highlighted surface can end in
られ rather than include the entire inflection. Focused analyzer tests cover
ichidan, godan, する, past and negative forms, ordinary passives and causatives,
lexical 見せる, and reservation. Dictionary-expression precedence and
eligible-example requirements apply.

## Limitations

Contracted godan forms such as 読まされた are not matched: Sudachi can analyze
読まさ as a complete verb, so splitting it would violate token boundaries and
risk suppressing a lexical reading. Other analyzer segmentation, inflections,
ambiguities and corpus precision and recall remain unassessed.
