"""Noun + にかけては: with regard to a particular field or skill."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nikakete.index import match_nikakete


def match_nikaketeha(tokens):
    ends = {end: start for start, end in match_nikakete(tokens)}
    for previous, particle in pairwise(tokens):
        start = ends.get(previous['span'][1])
        if (start is not None and particle['surface'] == 'は'
                and particle['pos'][:1] == ['助詞'] and adjacent(previous, particle)):
            yield start, particle['span'][1]


RULE = GrammarRule(order=-1, id='nikaketeha', expression='〜にかけては',
                   reading='にかけては', meaning='when it comes to; in the field of',
                   match=match_nikaketeha)
RULES = (RULE,)
