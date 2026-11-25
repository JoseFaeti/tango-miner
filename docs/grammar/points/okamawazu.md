# 〜を構わず

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

Regardless of; without regard for.

## Verified coverage

Matches adjacent を構わず and をかまわず after a source token, including
nouns and nominalized clauses. A following に is allowed; the card span ends
at ず. The preceding word remains available as lexical evidence.

Sudachi full-dictionary tests verify both spellings, nominalized clauses,
optional following に, whitespace and punctuation boundaries, and controls
for positive 構う, 構わない and the separate にかまわず construction.
Integration tests verify reservation, example surface and canonical identity.

Literal “without attending to” readings of 構う are retained: this rule does
not distinguish them contextually from “regardless of”. No following clause
is required. Intervening particles and other analyzer tokenizations remain
unsupported or unverified. See [〜にかまわず](nikamawazu.md) for the に variant.
