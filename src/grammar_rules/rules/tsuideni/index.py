"""Taking the opportunity to do something else."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TSUIDENI_EXPRESSION = '〜ついでに'


def match_tsuideni(tokens):
    for noun, particle in pairwise(tokens):
        if (noun['surface'] == 'ついで' and noun['lemma'] == 'ついで'
                and noun['pos'][:1] == ['名詞']
                and particle['surface'] == 'に' and particle['lemma'] == 'に'
                and particle['pos'][:2] == ['助詞', '格助詞']
                and adjacent(noun, particle)):
            yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=105, id='tsuideni', expression=TSUIDENI_EXPRESSION,
                   reading='ついでに', meaning='taking the opportunity; while at it',
                   match=match_tsuideni)
RULES = (RULE,)
