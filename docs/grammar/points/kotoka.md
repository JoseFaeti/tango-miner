# 〜ことか

Status: [~]

## Meaning

Expresses strong feeling about the degree or extent of something: “how…” or
“what a…”. For example, どんなに嬉しいことか and 何度後悔したことか.

## Implemented coverage

Matches sentence-final ことか and 事か after an attributive verb, adjective,
or auxiliary, including past and な-adjective forms. The card highlights only
ことか. Sudachi full-dictionary tokenizations and focused tests cover these
forms, embedded questions, lexical controls, and card integration.

## Limitations

The same ending can be a literal question (する事か); tokenization alone cannot
reliably distinguish it from an exclamation, so such cases may be included.
Embedded or quoted exclamations followed by a clause are currently missed.
