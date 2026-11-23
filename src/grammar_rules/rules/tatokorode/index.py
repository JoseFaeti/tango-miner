"""Concessive past-verb たところで."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.tokorode.index import match_tokorode

RULE = GrammarRule(order=106, id='tatokorode', expression='〜たところで',
                   reading='たところで', meaning='even if; even though',
                   match=match_tokorode)
RULES = (RULE,)
