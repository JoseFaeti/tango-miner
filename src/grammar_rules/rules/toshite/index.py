"""Noun + として: in the role or capacity of."""
TOSHITE_EXPRESSION = '〜として'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toshite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, to, shi, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] != ['名詞']
                or [(t['surface'], t['pos'][:1]) for t in (to, shi, te)] !=
                [('と', ['助詞']), ('し', ['動詞']), ('て', ['助詞'])]
                or shi['lemma'] != 'する'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        yield to['span'][0], te['span'][1]


RULE = GrammarRule(order=16, id='toshite', expression=TOSHITE_EXPRESSION,
                   reading='として', meaning='as; in the capacity of',
                   match=match_toshite)

RULES = (RULE,)
