"""Emphatic literary topic marker following a nominal expression."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TARUYA_EXPRESSION = '〜たるや'


def match_taruya(tokens):
    for i in range(1, len(tokens) - 1):
        before, copula, particle = tokens[i - 1:i + 2]
        nominal = (before['pos'][:1] in (['名詞'], ['代名詞'])
                   or before['pos'][:2] == ['接尾辞', '名詞的'])
        if (nominal and copula['surface'] == 'たる' and copula['lemma'] == 'たり'
                and copula['pos'][:1] == ['助動詞']
                and particle['surface'] == 'や' and particle['lemma'] == 'や'
                and particle['pos'][:1] == ['助詞']
                and adjacent(before, copula) and adjacent(copula, particle)):
            yield copula['span'][0], particle['span'][1]


RULE = GrammarRule(id='taruya', expression=TARUYA_EXPRESSION,
                   reading='たるや', meaning='as for; speaking of (emphatic, literary)',
                   match=match_taruya)
RULES = (RULE,)
