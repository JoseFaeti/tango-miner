"""Noun + をよそに: disregarding or contrary to something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_oyosoni(tokens):
    for i in range(1, len(tokens) - 2):
        previous, wo, yoso, ni = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or yoso['surface'] not in ('よそ', '余所') or yoso['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip((previous, wo, yoso), (wo, yoso, ni)))):
            continue
        yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(order=1, id='oyosoni', expression='〜をよそに',
                   reading='をよそに', meaning='disregarding; contrary to',
                   match=match_oyosoni)
RULES = (RULE,)
