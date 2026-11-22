"""Noun + をもちまして: formal indication of a time or means."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.omotsute.index import match_omotsute

OMOCHIMASHITE_EXPRESSION = '〜をもちまして'


def match_omochimashite(tokens):
    for start, end in match_omotsute(tokens):
        surfaces = [t['surface'] for t in tokens
                    if start <= t['span'][0] and t['span'][1] <= end]
        if surfaces in (['を', 'もち', 'まし', 'て'], ['を', '持ち', 'まし', 'て']):
            yield start, end


RULE = GrammarRule(order=-2, id='omochimashite', expression=OMOCHIMASHITE_EXPRESSION,
                   reading='をもちまして', meaning='as of; with; by means of (formal)',
                   match=match_omochimashite)
RULES = (RULE,)
