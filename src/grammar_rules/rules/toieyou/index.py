"""Tentative assertion with と言えよう."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toieyou(tokens):
    for to, verb in pairwise(tokens):
        if (to['surface'] == 'と' and to['pos'][:2] == ['助詞', '格助詞']
                and verb['surface'] in ('言えよう', 'いえよう')
                and verb['lemma'] in ('言える', 'いえる')
                and verb['pos'][:1] == ['動詞'] and adjacent(to, verb)):
            yield to['span'][0], verb['span'][1]


RULE = GrammarRule(order=2, id='toieyou', expression='〜といえよう',
                   reading='といえよう', meaning='one could say; it may be said',
                   match=match_toieyou)
RULES = (RULE,)
