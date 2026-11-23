"""Repeated だの listing markers."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_danodano(tokens):
    markers = []
    for i in range(1, len(tokens) - 1):
        before, da, no = tokens[i - 1:i + 2]
        if (before['pos'][:1] in (['名詞'], ['代名詞'], ['動詞'], ['形容詞'], ['助動詞'])
                and da['surface'] == 'だ' and da['pos'][:1] == ['助動詞']
                and no['surface'] == 'の' and no['pos'][:1] == ['助詞']
                and adjacent(before, da) and adjacent(da, no)):
            markers.append(i)
    for first, second in pairwise(markers):
        between = tokens[first + 2:second]
        if not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n') for t in between):
            continue
        yield tokens[first]['span'][0], tokens[first + 1]['span'][1]


RULE = GrammarRule(order=2, id='danodano', expression='〜だの〜だの',
                   reading='だのだの', meaning='things like ... and ...; ... and so on',
                   match=match_danodano)
RULES = (RULE,)
