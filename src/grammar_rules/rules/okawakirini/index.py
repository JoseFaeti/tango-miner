"""Noun + を皮切りに: the first in a series."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKAWAKIRINI_EXPRESSION = '〜を皮切りに'


def match_okawakirini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, trigger, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and trigger['surface'] in ('皮切り',)
                and trigger['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, trigger), (wo, trigger, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='okawakirini', expression=OKAWAKIRINI_EXPRESSION,
                   reading='を皮切りに', meaning='starting with; beginning a series with',
                   match=match_okawakirini)
RULES = (RULE,)
