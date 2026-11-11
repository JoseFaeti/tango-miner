"""Sentence-final elliptical negative conditional obligations."""
from src.GrammarConstants import NAKUTEHA_NAKEREBA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.elliptical_obligation.index import match_final_obligation


def match_nakuteha_nakereba(tokens):
    return match_final_obligation(tokens, ('なくては', 'なければ'))


RULE = GrammarRule(
    id='nakuteha_nakereba', expression=NAKUTEHA_NAKEREBA_EXPRESSION,
    reading='なくては / なければ', meaning='must; have to (elliptical)',
    match=match_nakuteha_nakereba,
)
