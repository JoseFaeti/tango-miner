"""Accompanying change expressed by noun or dictionary verb + に伴って."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜に伴って'


def match_nitomonatte(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, te = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (preceding['pos'][:1] == ['動詞']
                    and preceding['surface'] != preceding['lemma'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('伴っ', 'ともなっ')
                or verb['lemma'] not in ('伴う', 'ともなう')
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (preceding, ni, verb), (ni, verb, te)))):
            continue
        yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=11, id='nitomonatte', expression=EXPRESSION,
                   reading='にともなって', meaning='as; along with',
                   match=match_nitomonatte)
RULES = (RULE,)
