"""Polite noun-attached topic marker につきましては."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitsukimashiteha(tokens):
    for i in range(1, len(tokens) - 3):
        noun, ni, tsuki, mashi = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                and noun['pos'][:2] != ['接尾辞', '名詞的']):
            continue
        if (ni['surface'] != 'に' or ni['pos'][:2] != ['助詞', '格助詞']
                or (tsuki['surface'], tsuki['lemma']) != ('つき', 'つく')
                or tsuki['pos'][:1] != ['動詞']
                or (mashi['surface'], mashi['lemma']) != ('まし', 'ます')
                or mashi['pos'][:1] != ['助動詞']):
            continue
        tail = tokens[i + 3:i + 4]
        if tail[0]['surface'] == 'て':
            tail = tokens[i + 3:i + 5]
            if (len(tail) != 2 or tail[1]['surface'] != 'は'
                    or tail[1]['pos'][:2] != ['助詞', '係助詞']):
                continue
        elif tail[0]['surface'] != 'ては':
            continue
        if tail[0]['pos'][:2] != ['助詞', '接続助詞']:
            continue
        phrase = [noun, ni, tsuki, mashi] + tail
        if all(adjacent(a, b) for a, b in pairwise(phrase)):
            yield ni['span'][0], tail[-1]['span'][1]


RULE = GrammarRule(id='nitsukimashiteha', expression='〜につきましては',
                   reading='につきましては', meaning='as for; regarding (polite)',
                   match=match_nitsukimashiteha, order=11)
RULES = (RULE,)
