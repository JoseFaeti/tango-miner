"""Emphatic にしてからが, including potentially literal readings."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitekaraga(tokens):
    for i in range(1, len(tokens) - 4):
        previous, ni, shi, te, kara, ga = tokens[i - 1:i + 5]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or [(t['surface'], t['pos'][:1]) for t in (ni, shi, te, kara, ga)] !=
                [('に', ['助詞']), ('し', ['動詞']), ('て', ['助詞']),
                 ('から', ['助詞']), ('が', ['助詞'])]
                or shi['lemma'] != 'する'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        yield ni['span'][0], ga['span'][1]


RULE = GrammarRule(order=16, id='nishitekaraga', expression='〜にしてからが',
                   reading='にしてからが', meaning='even; to begin with',
                   match=match_nishitekaraga)
RULES = (RULE,)
