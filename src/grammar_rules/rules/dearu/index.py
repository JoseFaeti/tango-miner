"""Formal copula with a split copular で and ある."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

DEARU_EXPRESSION = '〜である'


def match_dearu(tokens):
    for i in range(1, len(tokens) - 1):
        previous, de, aru = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or not adjacent(previous, de) or not adjacent(de, aru)
                or (de['surface'], de['lemma'], de['pos'][:1]) != ('で', 'だ', ['助動詞'])
                or aru['lemma'] != 'ある' or aru['pos'][:1] != ['動詞']):
            continue
        if aru['surface'] == 'ある':
            end = i + 1
        elif aru['surface'] == 'あっ' and next_matches(tokens, i + 1, 'た', '助動詞', 'た'):
            end = i + 2
        elif aru['surface'] == 'あれ' and next_matches(tokens, i + 1, 'ば', '助詞', 'ば'):
            end = i + 2
        else:
            continue
        yield de['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='dearu', expression=DEARU_EXPRESSION, reading='である',
                   meaning='is; be (formal copula)', match=match_dearu)
RULES = (RULE,)
