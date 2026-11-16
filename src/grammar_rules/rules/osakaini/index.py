"""Noun + を境に: marking a boundary or turning point."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OSAKAINI_EXPRESSION = '〜を境に'


def match_osakaini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, boundary, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and boundary['surface'] == '境' and boundary['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, boundary), (wo, boundary, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='osakaini', expression=OSAKAINI_EXPRESSION,
                   reading='をさかいに', meaning='taking as a boundary or turning point',
                   match=match_osakaini)
RULES = (RULE,)
