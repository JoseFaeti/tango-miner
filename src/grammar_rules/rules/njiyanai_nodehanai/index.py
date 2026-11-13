"""Negative explanatory copula, including conjectural and prohibitive uses."""
NJIYANAI_NODEHANAI_EXPRESSION = '〜んじゃない / 〜のではない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nda.index import match_nda


def match_njiyanai_nodehanai(tokens):
    for start, end in match_nda(tokens):
        surface = ''.join(t['surface'] for t in tokens
                          if start <= t['span'][0] and t['span'][1] <= end)
        if surface.startswith(('んじゃ', 'んでは', 'のじゃ', 'のでは')):
            # The shared rule also accepts affirmative ではある / じゃある.
            if not surface.startswith(('んではある', 'のではある', 'んじゃある', 'のじゃある')):
                yield start, end


RULE = GrammarRule(order=65, id='njiyanai_nodehanai', expression=NJIYANAI_NODEHANAI_EXPRESSION,
                   reading='んじゃない / のではない',
                   meaning='explanatory negation; perhaps; do not (depending on context)',
                   match=match_njiyanai_nodehanai)

RULES = (RULE,)
