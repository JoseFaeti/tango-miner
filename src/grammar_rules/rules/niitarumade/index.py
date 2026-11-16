"""Noun + に至るまで: extending to and including an endpoint."""
NIITARUMADE_EXPRESSION = '〜に至るまで'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_niitarumade(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, made = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('至る', 'いたる')
                and verb['lemma'] in ('至る', 'いたる') and verb['pos'][:1] == ['動詞']
                and made['surface'] == 'まで' and made['lemma'] == 'まで'
                and made['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, made)))):
            yield ni['span'][0], made['span'][1]


RULE = GrammarRule(order=1, id='niitarumade', expression=NIITARUMADE_EXPRESSION,
                   reading='にいたるまで', meaning='up to and including',
                   match=match_niitarumade)

RULES = (RULE,)
