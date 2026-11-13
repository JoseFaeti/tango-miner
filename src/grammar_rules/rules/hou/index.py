"""Verb-stem + 方, expressing a way of doing something."""
HOU_EXPRESSION = '〜方'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_hou(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        if (adjacent(verb, suffix) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5] == '連用形-一般'
                and suffix['surface'] in ('方', 'かた')
                and suffix['pos'][:2] == ['接尾辞', '名詞的']):
            yield suffix['span'][0], suffix['span'][1]


RULE = GrammarRule(order=182, id='hou', expression=HOU_EXPRESSION, reading='かた',
                   meaning='way of doing', match=match_hou)

RULES = (RULE,)
