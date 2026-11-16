"""Noun + を背景に: circumstances forming a background."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OHAIKEINI_EXPRESSION = '〜を背景に'


def match_ohaikeini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, trigger, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and trigger['surface'] == '背景'
                and trigger['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, trigger), (wo, trigger, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='ohaikeini', expression=OHAIKEINI_EXPRESSION,
                   reading='をはいけいに', meaning='against the background of; influenced by',
                   match=match_ohaikeini)
RULES = (RULE,)
