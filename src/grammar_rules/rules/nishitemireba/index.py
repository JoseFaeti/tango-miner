"""Perspective expressed with noun + にしてみれば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NISHITEMIREBA_EXPRESSION = '〜にしてみれば'


def match_nishitemireba(tokens):
    for i in range(1, len(tokens) - 4):
        previous, ni, shi, te, mire, ba = tokens[i - 1:i + 5]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or [(t['surface'], t['pos'][0]) for t in (ni, shi, te, mire, ba)]
                != [('に', '助詞'), ('し', '動詞'), ('て', '助詞'),
                    ('みれ', '動詞'), ('ば', '助詞')]
                or shi['lemma'] != 'する' or mire['lemma'] != 'みる'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        yield ni['span'][0], ba['span'][1]


RULE = GrammarRule(id='nishitemireba', expression=NISHITEMIREBA_EXPRESSION,
                   reading='にしてみれば', meaning='from the perspective of',
                   match=match_nishitemireba, order=12)
RULES = (RULE,)
