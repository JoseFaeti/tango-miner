"""Formal expression for combining an occasion with another purpose."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KATAGATA_EXPRESSION = '〜かたがた'


def match_katagata(tokens):
    for i in range(1, len(tokens) - 1):
        previous, token, following = tokens[i - 1:i + 2]
        if (previous['pos'][:1] == ['名詞']
                and token['surface'] in ('かたがた', '方々')
                and token['lemma'] == token['surface']
                and token['pos'][:1] == ['名詞']
                and following['pos'][:1] not in (['助詞'], ['助動詞'], ['補助記号'])
                and adjacent(previous, token) and adjacent(token, following)):
            yield token['span'][0], token['span'][1]


RULE = GrammarRule(order=86, id='katagata', expression=KATAGATA_EXPRESSION,
                   reading='かたがた', meaning='on the occasion of; combining one purpose with another',
                   match=match_katagata)
RULES = (RULE,)
