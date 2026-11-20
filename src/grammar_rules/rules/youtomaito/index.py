"""Paired positive and negative alternatives marked by と."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.yougamaiga.index import match_paired_alternatives


def match_youtomaito(tokens):
    yield from match_paired_alternatives(tokens, 'と')


RULE = GrammarRule(id='youtomaito', expression='〜ようと〜まいと',
                   reading='ようとまいと', meaning='whether or not',
                   match=match_youtomaito, order=2)
RULES = (RULE,)
