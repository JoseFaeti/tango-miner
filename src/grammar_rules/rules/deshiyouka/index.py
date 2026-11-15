"""Polite questions ending in でしょうか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.darouka.index import match_darouka

DESHIYOUKA_EXPRESSION = '〜でしょうか'


def match_deshiyouka(tokens):
    for copula, ka in zip(tokens, tokens[1:]):
        if copula['surface'] == 'でしょう':
            yield from match_darouka((copula, ka))


RULE = GrammarRule(order=60, id='deshiyouka', expression=DESHIYOUKA_EXPRESSION,
                   reading='でしょうか', meaning='polite question; I wonder whether',
                   match=match_deshiyouka)
RULES = (RULE,)
