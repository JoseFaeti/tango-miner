"""Adverbial まして, expressing an even stronger inference."""
from src.grammar_rules.base import GrammarRule

MASHITE_EXPRESSION = '〜まして'


def match_mashite(tokens):
    for token in tokens:
        if (token['surface'] == 'まして' and token['lemma'] == 'まして'
                and token['pos'][:1] == ['副詞']):
            yield token['span'][0], token['span'][1]


RULE = GrammarRule(order=2, id='mashite', expression=MASHITE_EXPRESSION,
                   reading='まして', meaning='all the more; still less',
                   match=match_mashite)
RULES = (RULE,)
