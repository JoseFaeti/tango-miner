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


def positive_verb_end(tokens, index, *, lemma, base_forms, past_stems, polite_stems):
    """Match selected positive finite forms; return the last token index or None.

    Callers supply the verb-specific surfaces and decide which meanings and
    forms are appropriate for their construction. Incomplete suffixes fail.
    """
    token = tokens[index]
    if token['lemma'] != lemma or token['pos'][:1] != ['動詞']:
        return None
    surface = token['surface']
    if surface in base_forms:
        return index
    if surface in past_stems and next_matches(tokens, index, 'た', '助動詞', 'た'):
        return index + 1
    if surface in polite_stems and next_matches(tokens, index, 'ます', '助動詞'):
        suffix = tokens[index + 1]['surface']
        if suffix == 'ます':
            return index + 1
        if suffix == 'まし' and next_matches(tokens, index + 1, 'た', '助動詞', 'た'):
            return index + 2
    return None
