"""Worthiness expressed by に値する."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    negative_suffix_end,
    next_matches,
    polite_suffix_end,
    positive_verb_end,
)


def match_niataisuru(tokens):
    for i in range(1, len(tokens) - 2):
        before, particle, value, verb = tokens[i - 1:i + 3]
        if not (before['pos'][:1] in (['名詞'], ['動詞'])
                and particle['surface'] == 'に' and particle['pos'][:1] == ['助詞']
                and value['surface'] == '値' and value['lemma'] == '値'
                and value['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in ((before, particle), (particle, value), (value, verb)))):
            continue
        end = positive_verb_end(tokens, i + 2, lemma='する', base_forms=('する',),
                                past_stems=('し',), polite_stems=('し',),
                                te_stems=('し',), conditional_stems=('すれ',))
        if (end is None and verb['lemma'] == 'する' and verb['pos'][:1] == ['動詞']
                and verb['surface'] == 'し'):
            if next_matches(tokens, i + 2, 'ない', '助動詞'):
                end = negative_suffix_end(tokens, i + 3, pos='助動詞', extended=True)
            elif next_matches(tokens, i + 2, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='niataisuru', expression='〜に値する', reading='にあたいする',
                   meaning='worthy of; deserving of', match=match_niataisuru)
RULES = (RULE,)
