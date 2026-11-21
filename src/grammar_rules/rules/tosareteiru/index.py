"""Conventional judgment in the resulting-state form とされている."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.te_iru.index import match_te_iru
from src.grammar_rules.rules.tosareru.index import match_tosareru


def match_tosareteiru(tokens):
    aspect = dict(match_te_iru(tokens))
    for start, end in match_tosareru(tokens):
        if end - 1 in aspect and any(
                token['surface'] == 'て' and token['span'][0] == end - 1 and token['span'][1] == end
                for token in tokens):
            yield start, aspect[end - 1]


RULE = GrammarRule(id='tosareteiru', expression='〜とされている', reading='とされている',
                   meaning='is regarded as; is generally considered to be',
                   match=match_tosareteiru)
RULES = (RULE,)
