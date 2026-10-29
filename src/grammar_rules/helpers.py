"""Helpers for source-aligned grammar token matching."""


def adjacent(first, second):
    """Whether two aligned tokens touch in the source."""
    return first['span'][1] == second['span'][0]


def has_next(tokens, index):
    """Whether the next token exists and touches the token at index."""
    return index + 1 < len(tokens) and adjacent(tokens[index], tokens[index + 1])


def next_matches(tokens, index, lemma, pos, surface=None):
    """Check an adjacent next token's lemma, coarse POS and optional surface."""
    return (has_next(tokens, index) and tokens[index + 1]['lemma'] == lemma
            and tokens[index + 1]['pos'][:1] == [pos]
            and (surface is None or tokens[index + 1]['surface'] == surface))
