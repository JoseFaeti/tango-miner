"""Clause-linking and introductory conjunctive particle が."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ga_conjunction(tokens):
    for previous, marker in pairwise(tokens):
        pos = previous['pos']
        if (marker['surface'] == 'が' and marker['lemma'] == 'が'
                and marker['pos'][:2] == ['助詞', '接続助詞']
                and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith('終止形')
                and adjacent(previous, marker)):
            yield tuple(marker['span'])


RULE = GrammarRule(order=106, id='ga_conjunction', expression='〜が (接続助詞)',
                   reading='が', meaning='but; although; introduction; softening',
                   match=match_ga_conjunction)
RULES = (RULE,)
