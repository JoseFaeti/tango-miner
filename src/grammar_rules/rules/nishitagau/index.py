"""Noun or pronoun + に従う: follow or comply with."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitagau(tokens):
    for i in range(1, len(tokens) - 1):
        preceding, ni, verb = tokens[i - 1:i + 2]
        if (preceding['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('従う', 'したがう')
                and verb['lemma'] in ('従う', 'したがう')
                and verb['pos'][:1] == ['動詞']
                and adjacent(preceding, ni) and adjacent(ni, verb)):
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=11, id='nishitagau', expression='〜に従う',
                   reading='にしたがう', meaning='follow; comply with; obey',
                   match=match_nishitagau)
RULES = (RULE,)
