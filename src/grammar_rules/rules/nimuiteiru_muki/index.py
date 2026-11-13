"""Suitability expressed with に向いている or the suffix 向き."""
NIMUITEIRU_MUKI_EXPRESSION = '〜に向いている / 〜向き'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.te_iru.index import match_te_iru


def match_nimuiteiru_muki(tokens):
    progressive = dict(match_te_iru(tokens))
    for i in range(1, len(tokens)):
        previous, token = tokens[i - 1:i + 1]
        if not adjacent(previous, token):
            continue
        if (previous['pos'][:1] in (['名詞'], ['代名詞'])
                and token['surface'] in ('向き', 'むき')
                and token['pos'][:2] == ['接尾辞', '名詞的']):
            yield token['span']
        if i < 2:
            continue
        noun = tokens[i - 2]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and previous['surface'] == 'に' and previous['pos'][:1] == ['助詞']
                and adjacent(noun, previous)
                and token['surface'] in ('向い', 'むい')
                and token['lemma'] in ('向く', 'むく') and token['pos'][:1] == ['動詞']
                and i + 1 < len(tokens) and adjacent(token, tokens[i + 1])):
            end = progressive.get(tokens[i + 1]['span'][0])
            if end is not None:
                yield previous['span'][0], end


RULE = GrammarRule(order=4, id='nimuiteiru_muki', expression=NIMUITEIRU_MUKI_EXPRESSION,
                   reading='にむいている / むき', meaning='suited to; suitable for',
                   match=match_nimuiteiru_muki)

RULES = (RULE,)
