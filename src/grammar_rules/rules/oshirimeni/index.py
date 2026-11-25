"""Noun + を尻目に: disregarding someone or something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_oshirimeni(tokens):
    for i in range(1, len(tokens) - 2):
        previous, wo, shirime, ni = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or shirime['surface'] not in ('尻目', 'しりめ') or shirime['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip((previous, wo, shirime), (wo, shirime, ni)))):
            continue
        yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(order=1, id='oshirimeni', expression='〜を尻目に',
                   reading='をしりめに', meaning='disregarding; with indifference to',
                   match=match_oshirimeni)
RULES = (RULE,)
