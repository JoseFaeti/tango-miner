"""Noun + に至って: upon reaching a point or stage."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIITATSUTE_EXPRESSION = '〜に至って'


def match_niitatsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('至っ', 'いたっ')
                and verb['lemma'] in ('至る', 'いたる') and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))
                and not (i + 3 < len(tokens) and adjacent(te, tokens[i + 3])
                         and tokens[i + 3]['surface'] == 'は'
                         and tokens[i + 3]['pos'][:1] == ['助詞'])):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=1, id='niitatsute', expression=NIITATSUTE_EXPRESSION,
                   reading='にいたって', meaning='upon reaching; only when',
                   match=match_niitatsute)

RULES = (RULE,)
