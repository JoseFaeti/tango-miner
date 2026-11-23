"""Passive 余儀なくされる following an object."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    adjective_inflection_end,
    next_matches,
    polite_suffix_end,
)


def match_oyoginakusareru(tokens):
    for i in range(1, len(tokens) - 4):
        before, particle, yogi, naku, sa, passive = tokens[i - 1:i + 5]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or yogi['surface'] != '余儀' or yogi['pos'][:1] != ['名詞']
                or naku['surface'] != 'なく' or naku['lemma'] != 'ない'
                or sa['surface'] != 'さ' or sa['lemma'] != 'する' or sa['pos'][:1] != ['動詞']
                or passive['lemma'] != 'れる' or passive['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        end = None
        surface = passive['surface']
        if surface == 'れる':
            end = i + 4
        elif surface == 'れ':
            j = i + 4
            if next_matches(tokens, j, 'た', '助動詞') or next_matches(tokens, j, 'て', '助詞', 'て'):
                end = j + 1
            elif next_matches(tokens, j, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, j + 1, forms=('positive', 'negative'))
            elif next_matches(tokens, j, 'ない', '助動詞'):
                end = adjective_inflection_end(tokens, j + 1, lemma='ない', pos='助動詞',
                    base='ない', past_stem='なかっ', continuative_stem='なく', conditional_stem='なけれ')
        elif surface == 'れれ' and next_matches(tokens, i + 4, 'ば', '助詞', 'ば'):
            end = i + 5
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='oyoginakusareru', expression='〜を余儀なくされる',
                   reading='をよぎなくされる', meaning='be forced to; have no choice but to',
                   match=match_oyoginakusareru, order=30)
RULES = (RULE,)
