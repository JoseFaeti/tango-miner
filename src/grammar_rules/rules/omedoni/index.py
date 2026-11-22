"""Noun + をめどに: aiming for an approximate target."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OMEDONI_EXPRESSION = '〜をめどに'


def match_omedoni(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, medo, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and medo['surface'] in ('めど', '目処', '目途')
                and medo['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, medo), (wo, medo, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='omedoni', expression=OMEDONI_EXPRESSION,
                   reading='をめどに', meaning='aiming for; with as an approximate target',
                   match=match_omedoni)
RULES = (RULE,)
