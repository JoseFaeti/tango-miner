"""Concessive にしても, retaining possible literal interpretations."""
NISHITEMO_EXPRESSION = '〜にしても'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitemo(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ni, shi, te, mo = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or [(t['surface'], t['pos'][:1]) for t in (ni, shi, te, mo)] !=
                [('に', ['助詞']), ('し', ['動詞']), ('て', ['助詞']), ('も', ['助詞'])]
                or shi['lemma'] != 'する'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield ni['span'][0], mo['span'][1]


RULE = GrammarRule(order=18, id='nishitemo', expression=NISHITEMO_EXPRESSION,
                   reading='にしても', meaning='even if; even assuming',
                   match=match_nishitemo)

RULES = (RULE,)
