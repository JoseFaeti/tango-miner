"""Noun + を除いて: except for."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ONOZOITE_EXPRESSION = '〜を除いて'


def match_onozoite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb, tail = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'を' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('除い', 'のぞい')
                and verb['lemma'] in ('除く', 'のぞく') and verb['pos'][:1] == ['動詞']
                and tail['surface'] == 'て' and tail['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, particle, verb), (particle, verb, tail)))):
            yield particle['span'][0], tail['span'][1]


RULE = GrammarRule(order=6, id='onozoite', expression=ONOZOITE_EXPRESSION,
                   reading='をのぞいて', meaning='except for', match=match_onozoite)
RULES = (RULE,)
