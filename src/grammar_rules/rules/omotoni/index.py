"""Noun + をもとに: using something as a basis."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OMOTONI_EXPRESSION = '〜をもとに'


def match_omotoni(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, moto, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and moto['surface'] in ('もと', '元', '基')
                and moto['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, moto), (wo, moto, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='omotoni', expression=OMOTONI_EXPRESSION,
                   reading='をもとに', meaning='based on; using as a basis',
                   match=match_omotoni)
RULES = (RULE,)
