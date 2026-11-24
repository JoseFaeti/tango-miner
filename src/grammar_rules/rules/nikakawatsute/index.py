"""Connective noun + にかかわって: involving or affecting."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nikakawaru.index import match_nikakawaru


def match_nikakawatsute(tokens):
    te_ends = {token['span'][1] for token in tokens
               if token['surface'] == 'て' and token['pos'][:1] == ['助詞']}
    for start, end in match_nikakawaru(tokens, include_te=True):
        if end in te_ends:
            yield start, end


RULE = GrammarRule(order=6, id='nikakawatsute', expression='〜にかかわって',
                   reading='にかかわって', meaning='involving; relating to; affecting',
                   match=match_nikakawatsute)
RULES = (RULE,)
