"""Attributive noun or dictionary verb + に伴う + noun."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜に伴う'


def match_nitomonau(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, following = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] in (['名詞'], ['代名詞'], ['動詞'])
                and (preceding['pos'][:1] != ['動詞']
                     or preceding['surface'] == preceding['lemma'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('伴う', 'ともなう')
                and verb['lemma'] in ('伴う', 'ともなう')
                and verb['pos'][:1] == ['動詞']
                and following['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in zip(
                    (preceding, ni, verb), (ni, verb, following)))):
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=11, id='nitomonau', expression=EXPRESSION,
                   reading='にともなう', meaning='associated with; accompanying',
                   match=match_nitomonau)
RULES = (RULE,)
