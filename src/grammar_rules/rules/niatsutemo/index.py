"""Formal noun + にあっても: even in the circumstances of."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niatsute.index import match_niatsute


def match_niatsutemo(tokens):
    endings = {end: start for start, end in match_niatsute(tokens)}
    for previous, mo in pairwise(tokens):
        start = endings.get(previous['span'][1])
        if (start is not None and mo['surface'] == 'も'
                and mo['pos'][:1] == ['助詞'] and adjacent(previous, mo)):
            yield start, mo['span'][1]


RULE = GrammarRule(order=1, id='niatsutemo', expression='〜にあっても',
                   reading='にあっても', meaning='even in the circumstances of',
                   match=match_niatsutemo)
RULES = (RULE,)
