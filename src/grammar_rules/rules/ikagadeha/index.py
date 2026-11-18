"""Noun + いかんでは: depending on the circumstances."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.ikande_ikanniyotteha.index import match_ikande_base

IKAGADEHA_EXPRESSION = '〜如何では'


def match_ikagadeha(tokens):
    for start, end in match_ikande_base(tokens):
        final = next((token for token in tokens if token['span'][1] == end), None)
        following = next((token for token in tokens if token['span'][0] == end), None)
        if (final and final['surface'] in ('で', '如何で', 'いかんで')
                and following and following['surface'] == 'は'
                and following['pos'][:1] == ['助詞']):
            yield start, following['span'][1]


RULE = GrammarRule(order=1, id='ikagadeha', expression=IKAGADEHA_EXPRESSION,
                   reading='いかんでは', meaning='depending on the circumstances',
                   match=match_ikagadeha)
RULES = (RULE,)
