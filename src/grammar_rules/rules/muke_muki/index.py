"""Nominal suffixes for an intended audience or suitability."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


MUKE_MUKI_EXPRESSION = '〜向け / 〜向き'


def match_muke_muki(tokens):
    for previous, token in zip(tokens, tokens[1:]):
        if (adjacent(previous, token)
                and previous['pos'][:1] in (['名詞'], ['代名詞'])
                and token['surface'] in ('向け', 'むけ', '向き', 'むき')
                and token['pos'][:2] == ['接尾辞', '名詞的']):
            yield token['span']


RULE = GrammarRule(order=4, id='muke_muki', expression=MUKE_MUKI_EXPRESSION,
                   reading='むけ / むき', meaning='intended for; suited to',
                   match=match_muke_muki)

RULES = (RULE,)
