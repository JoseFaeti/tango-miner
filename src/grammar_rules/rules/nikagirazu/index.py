"""Noun + に限らず: not limited to the named example."""
NIKAGIRAZU_EXPRESSION = '〜に限らず'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikagirazu(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, zu = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('限ら', 'かぎら')
                and verb['lemma'] in ('限る', 'かぎる') and verb['pos'][:1] == ['動詞']
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, zu)))):
            yield ni['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='nikagirazu', expression=NIKAGIRAZU_EXPRESSION,
                   reading='にかぎらず', meaning='not limited to; not only',
                   match=match_nikagirazu)

RULES = (RULE,)
