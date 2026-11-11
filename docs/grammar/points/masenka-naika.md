# 〜ませんか / 〜ないか

Status: [~]

## Meaning

Invitation.

## Supported behavior

Recognizes sentence-final verbal ませんか and ないか, including polite and
plain invitations such as 一緒に行きませんか and 一緒に行かないか. The
matched span is the ending, leaving the verb available as lexical evidence.
The rule requires adjacent analyzer tokens and a sentence-final か; it excludes
embedded questions, adjectival ない, and ないかな. Focused tests cover actual
Sudachi tokenization, lexical controls, boundaries, and card integration.

Negative questions can also ask for information or express surprise; syntax
alone cannot reliably distinguish them from invitations, so such cases may
produce this card. Sentence-final particles after か and other variants are
not yet supported. The existing negative-ending rules can also match the same
occurrence; longer dictionary expressions retain the usual precedence.
