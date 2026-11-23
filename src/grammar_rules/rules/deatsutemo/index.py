"""Concessive form of the formal copula."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.deatsute.index import match_deatsute


def match_deatsutemo(tokens):
    endings = {end: start for start, end in match_deatsute(tokens)}
    for previous, mo in itertools.pairwise(tokens):
        start = endings.get(previous['span'][1])
        if (start is not None and mo['surface'] == 'も'
                and mo['pos'][:1] == ['助詞'] and adjacent(previous, mo)):
            yield start, mo['span'][1]


RULE = GrammarRule(order=1, id='deatsutemo', expression='〜であっても',
                   reading='であっても', meaning='even if; even though (formal copula)',
                   match=match_deatsutemo)
RULES = (RULE,)
