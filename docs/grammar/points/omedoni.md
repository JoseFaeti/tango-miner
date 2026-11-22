# 〜をめどに

Status: [~]

## Meaning

Taking something as an approximate target or goal.

## Implemented coverage

Matches a noun or pronoun followed by をめどに, を目処に, or を目途に,
including the prefix of をめどにして. The reserved span and card example run
from を through に, leaving the preceding noun available as a lexical candidate.
Focused tests cover Sudachi full-dictionary tokenization, nearby nonmatches,
reservation and card identity.

The matcher does not verify whether the preceding noun makes sense as a target.
Other spellings, tokenizations and related forms such as をめどとする have not
been implemented; coverage remains partial.
