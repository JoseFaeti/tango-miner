"""Noun or pronoun + を踏まえて: taking into account."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OFUMAETE_EXPRESSION = '〜を踏まえて'


def match_ofumaete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and verb['surface'] in ('踏まえ', 'ふまえ')
                and verb['lemma'] in ('踏まえる', 'ふまえる')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, verb), (wo, verb, te)))):
            yield wo['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='ofumaete', expression=OFUMAETE_EXPRESSION,
                   reading='をふまえて', meaning='taking into account',
                   match=match_ofumaete)
RULES = (RULE,)
