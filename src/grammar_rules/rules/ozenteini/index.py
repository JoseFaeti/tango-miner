"""Noun + を前提に: on the assumption of something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OZENTEINI_EXPRESSION = '〜を前提に'


def match_ozenteini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, zentei, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and zentei['surface'] == '前提' and zentei['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, zentei), (wo, zentei, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='ozenteini', expression=OZENTEINI_EXPRESSION,
                   reading='をぜんていに', meaning='on the assumption that; assuming',
                   match=match_ozenteini)
RULES = (RULE,)
