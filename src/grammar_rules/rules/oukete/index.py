"""Noun or pronoun + を受けて: in response to; following."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OUKETE_EXPRESSION = '〜を受けて'


def match_oukete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and verb['surface'] in ('受け', 'うけ')
                and verb['lemma'] in ('受ける', 'うける')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, verb), (wo, verb, te)))):
            yield wo['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='oukete', expression=OUKETE_EXPRESSION,
                   reading='をうけて', meaning='in response to; following',
                   match=match_oukete)
RULES = (RULE,)
