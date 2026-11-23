"""Formal noun + にあっては: in the case or circumstances of."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niatsute.index import match_niatsute


def match_niatsuteha(tokens):
    endings = {end: start for start, end in match_niatsute(tokens)}
    for previous, ha in pairwise(tokens):
        start = endings.get(previous['span'][1])
        if (start is not None and ha['surface'] == 'は'
                and ha['pos'][:1] == ['助詞'] and adjacent(previous, ha)):
            yield start, ha['span'][1]


RULE = GrammarRule(order=1, id='niatsuteha', expression='〜にあっては',
                   reading='にあっては', meaning='in the case of; in the circumstances of',
                   match=match_niatsuteha)
RULES = (RULE,)
