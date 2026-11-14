"""Noun + だらけ, covered or filled with something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

DARAKE_EXPRESSION = '〜だらけ'


def match_darake(tokens):
    for i in range(1, len(tokens)):
        noun, suffix = tokens[i - 1:i + 1]
        if (noun['pos'][:1] == ['名詞'] and adjacent(noun, suffix)
                and suffix['surface'] == suffix['lemma'] == 'だらけ'
                and suffix['pos'][:2] == ['接尾辞', '形状詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='darake', expression=DARAKE_EXPRESSION,
                   reading='だらけ', meaning='covered with; full of',
                   match=match_darake)
RULES = (RULE,)
