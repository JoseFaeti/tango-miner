"""Noun + を中心に: centered on or mainly involving something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ochiyuushinni(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, center, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or center['surface'] != '中心' or center['lemma'] != '中心'
                or center['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, particle, center), (particle, center, ni)))):
            continue
        yield particle['span'][0], ni['span'][1]


RULE = GrammarRule(order=7, id='ochiyuushinni', expression='〜を中心に',
                   reading='をちゅうしんに', meaning='centered on; mainly involving',
                   match=match_ochiyuushinni)

RULES = (RULE,)
