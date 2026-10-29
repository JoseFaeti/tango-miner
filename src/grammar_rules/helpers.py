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


def polite_suffix_end(tokens, index, *, forms=('positive', 'negative', 'te', 'volitional')):
    """Return the end of a complete supported ます suffix, or None.

    The caller selects meanings/forms appropriate to its construction.
    """
    token = tokens[index]
    if token['lemma'] != 'ます' or token['pos'][:1] != ['助動詞']:
        return None
    surface = token['surface']
    if surface in ('ます', 'ました') and 'positive' in forms:
        return index
    if surface in ('ません', 'ませんでした') and 'negative' in forms:
        return index
    if surface in ('ましょう', 'ましょ') and 'volitional' in forms:
        return index
    if surface == 'まし':
        if 'positive' in forms and next_matches(tokens, index, 'た', '助動詞', 'た'):
            return index + 1
        if 'te' in forms and next_matches(tokens, index, 'て', '助詞', 'て'):
            return index + 1
    if surface == 'ませ' and 'negative' in forms and next_matches(tokens, index, 'ぬ', '助動詞', 'ん'):
        end = index + 1
        if next_matches(tokens, end, 'です', '助動詞', 'でし'):
            return end + 2 if next_matches(tokens, end + 1, 'た', '助動詞', 'た') else None
        return end
    return None


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
        return polite_suffix_end(tokens, index + 1, forms=('positive',))
    return None


def negative_suffix_end(tokens, index, *, pos, continuative=False, conditional=False, extended=False):
    """Return the end of a supported negative suffix, requiring complete forms."""
    token = tokens[index]
    if token['lemma'] != 'ない' or token['pos'][:1] != [pos]:
        return None
    surface = token['surface']
    if surface == 'ない' or (continuative and surface == 'なく'):
        return index
    if surface == 'なかっ' and (next_matches(tokens, index, 'た', '助動詞', 'た')
                                 or (extended and next_matches(tokens, index, 'た', '助動詞'))):
        return index + 1
    if extended and surface == 'なくっ':
        return index + 1 if next_matches(tokens, index, 'て', '助詞', 'て') else None
    if (conditional or extended) and surface == 'なけれ':
        return index + 1 if next_matches(tokens, index, 'ば', '助詞', 'ば') else None
    if extended and surface in ('なきゃ', 'なけりゃ', 'なかろう', 'んかっ', 'な'):
        return index
    return None


def adjective_inflection_end(tokens, index, *, lemma, pos, base, past_stem,
                              continuative_stem, conditional_stem,
                              forms=('base', 'past', 'continuative', 'negative', 'te', 'conditional')):
    """Match selected adjective-like forms; return the last token index.

    The caller supplies the adjective's surfaces and selects applicable forms.
    A match describes morphology only, not the construction's meaning.
    """
    token = tokens[index]
    if token['lemma'] != lemma or token['pos'][:1] != [pos]:
        return None
    surface = token['surface']
    if surface == base and 'base' in forms:
        return index
    if surface == past_stem and 'past' in forms:
        return index + 1 if next_matches(tokens, index, 'た', '助動詞') else None
    if surface == conditional_stem and 'conditional' in forms:
        return index + 1 if next_matches(tokens, index, 'ば', '助詞', 'ば') else None
    if surface == continuative_stem:
        if 'negative' in forms and next_matches(tokens, index, 'ない', '形容詞'):
            return negative_suffix_end(tokens, index + 1, pos='形容詞', continuative=True, extended=True)
        if 'te' in forms and next_matches(tokens, index, 'て', '助詞', 'て'):
            return index + 1
        if 'continuative' in forms:
            return index
    return None


def negative_predicate_end(tokens, index, *, polite_lemma, polite_stems,
                           plain_lemma=None, plain_stems=(), continuative=False, extended=False):
    """Match ない / なかった or a supplied verb's ません / ませんでした.

    Return the final token index, or None for unsupported or incomplete forms.
    Callers are responsible for validating the surrounding construction.
    """
    token = tokens[index]
    if (token['lemma'] == plain_lemma and token['pos'][:1] == ['動詞']
            and token['surface'] in plain_stems
            and next_matches(tokens, index, 'ない', '助動詞')):
        return negative_suffix_end(tokens, index + 1, pos='助動詞', continuative=continuative, extended=extended)
    if token['lemma'] == 'ない' and token['pos'][:1] == ['形容詞']:
        return negative_suffix_end(tokens, index, pos='形容詞')
    if (token['lemma'] != polite_lemma or token['pos'][:1] != ['動詞']
            or token['surface'] not in polite_stems
            or not next_matches(tokens, index, 'ます', '助動詞')):
        return None
    return polite_suffix_end(tokens, index + 1, forms=('negative',))
