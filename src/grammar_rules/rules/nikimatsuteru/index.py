"""Colloquial strong conviction expressed by に決まってる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nikimatsuteiru.index import match_nikimatsu


def match_nikimatsuteru(tokens):
    return match_nikimatsu(tokens, contracted=True)


RULE = GrammarRule(order=36, id='nikimatsuteru', expression='〜に決まってる',
                   reading='にきまってる', meaning='surely; must be', match=match_nikimatsuteru)
RULES = (RULE,)
