"""Perspective expressed with noun + にしてみると."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NISHITEMIRUTO_EXPRESSION = '〜にしてみると'


def match_nishitemiruto(tokens):
    for i in range(1, len(tokens) - 4):
        previous, ni, shi, te, miru, to = tokens[i - 1:i + 5]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or [(t['surface'], t['pos'][0]) for t in (ni, shi, te, miru, to)]
                != [('に', '助詞'), ('し', '動詞'), ('て', '助詞'),
                    ('みる', '動詞'), ('と', '助詞')]
                or shi['lemma'] != 'する' or miru['lemma'] != 'みる'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        yield ni['span'][0], to['span'][1]


RULE = GrammarRule(id='nishitemiruto', expression=NISHITEMIRUTO_EXPRESSION,
                   reading='にしてみると', meaning='from the perspective of',
                   match=match_nishitemiruto, order=12)
RULES = (RULE,)
