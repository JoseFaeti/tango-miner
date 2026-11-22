"""Noun + に代わる: serving as a replacement."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAWARU_EXPRESSION = '〜に代わる'


def match_nikawaru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, particle, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'に' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('代わる', 'かわる')
                and verb['lemma'] in ('代わる', 'かわる')
                and verb['pos'][:1] == ['動詞']
                and adjacent(noun, particle) and adjacent(particle, verb)):
            yield particle['span'][0], verb['span'][1]


RULE = GrammarRule(order=0, id='nikawaru', expression=NIKAWARU_EXPRESSION,
                   reading='にかわる', meaning='replacing; in place of',
                   match=match_nikawaru)
RULES = (RULE,)
