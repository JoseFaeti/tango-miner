"""First occurrence at a temporal milestone: にして初めて."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nishite.index import match_nishite


def match_nishitehajimete(tokens):
    starts = {end: start for start, end in match_nishite(tokens)}
    for te, first in itertools.pairwise(tokens):
        start = starts.get(te['span'][1])
        if (start is not None and adjacent(te, first)
                and first['surface'] in ('初めて', 'はじめて')
                and first['pos'][:1] == ['副詞']):
            yield start, first['span'][1]


RULE = GrammarRule(id='nishitehajimete', expression='〜にして初めて',
                   reading='にしてはじめて', meaning='only after; not until',
                   match=match_nishitehajimete, order=19)
RULES = (RULE,)
