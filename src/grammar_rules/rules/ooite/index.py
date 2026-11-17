"""Exclusionary noun + をおいて, with an explicit ほか following it."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


OOITE_EXPRESSION = '〜をおいて'


def match_ooite(tokens):
    for i in range(1, len(tokens) - 3):
        noun, particle, verb, te, other = tokens[i - 1:i + 4]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'を' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('おい', '置い') and verb['lemma'] in ('おく', '置く')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and other['surface'] in ('ほか', '他', '外')
                and all(adjacent(a, b) for a, b in zip(
                    (noun, particle, verb, te), (particle, verb, te, other)))):
            yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='ooite', expression=OOITE_EXPRESSION,
                   reading='をおいて', meaning='apart from; other than', match=match_ooite)
RULES = (RULE,)
