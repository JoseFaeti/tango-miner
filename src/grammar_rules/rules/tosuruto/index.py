"""Conditional supposition with とすると."""
TOSURUTO_EXPRESSION = '〜とすると'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tosuruto(tokens):
    for i in range(1, len(tokens) - 2):
        previous, to, verb, ending = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or verb['surface'] != 'する' or verb['lemma'] != 'する'
                or verb['pos'][:1] != ['動詞']
                or ending['surface'] != 'と' or ending['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        yield to['span'][0], ending['span'][1]


RULE = GrammarRule(order=13, id='tosuruto', expression=TOSURUTO_EXPRESSION,
                   reading='とすると', meaning='assuming that; if so',
                   match=match_tosuruto)

RULES = (RULE,)
