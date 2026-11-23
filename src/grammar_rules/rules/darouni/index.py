"""Conjecture followed by contrastive に."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

DAROUNI_EXPRESSION = '〜だろうに'


def match_darouni(tokens):
    for copula, ni in pairwise(tokens):
        if ((copula['lemma'], copula['surface']) in (('だ', 'だろう'), ('です', 'でしょう'))
                and copula['pos'][:1] == ['助動詞']
                and ni['surface'] == ni['lemma'] == 'に'
                and ni['pos'][:2] in (['助詞', '接続助詞'], ['助詞', '格助詞'])
                and adjacent(copula, ni)):
            yield copula['span'][0], ni['span'][1]


RULE = GrammarRule(order=60, id='darouni', expression=DAROUNI_EXPRESSION,
                   reading='だろうに', meaning='would surely; probably, and yet',
                   match=match_darouni)
RULES = (RULE,)
