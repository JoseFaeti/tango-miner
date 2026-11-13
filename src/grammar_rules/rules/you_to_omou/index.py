"""Volitional verb + と思う: intention."""
YOU_TO_OMOU_EXPRESSION = '〜ようと思う'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.to_omou.index import _match_to_omou


def match_you_to_omou(tokens):
    yield from _match_to_omou(tokens, intention=True)


RULE = GrammarRule(order=94, id='you_to_omou', expression=YOU_TO_OMOU_EXPRESSION,
                   reading='ようとおもう', meaning='intend to do',
                   match=match_you_to_omou)

RULES = (RULE,)
