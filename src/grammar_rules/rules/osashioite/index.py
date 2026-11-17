"""Noun + を差し置いて: passing over or taking priority over someone."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


OSASHIOITE_EXPRESSION = '〜を差し置いて'


def match_osashioite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle = tokens[i - 1:i + 1]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or not adjacent(noun, particle)):
            continue
        patterns = (
            (('差し置い', '差し置く', '動詞'), ('て', 'て', '助詞')),
            (('さしおい', 'さしおく', '動詞'), ('て', 'て', '助詞')),
            (('差し', '差す', '動詞'), ('おい', 'おく', '動詞'), ('て', 'て', '助詞')),
            (('差し置き', '差し置く', '動詞'), ('まし', 'ます', '助動詞'), ('て', 'て', '助詞')),
            (('さしおき', 'さしおく', '動詞'), ('まし', 'ます', '助動詞'), ('て', 'て', '助詞')),
        )
        for pattern in patterns:
            tail = tokens[i + 1:i + 1 + len(pattern)]
            if (len(tail) == len(pattern)
                    and [(t['surface'], t['lemma'], t['pos'][0]) for t in tail] == list(pattern)
                    and all(adjacent(a, b) for a, b in zip([particle] + tail, tail))):
                yield particle['span'][0], tail[-1]['span'][1]
                break


RULE = GrammarRule(order=6, id='osashioite', expression=OSASHIOITE_EXPRESSION,
                   reading='をさしおいて', meaning='passing over; taking priority over',
                   match=match_osashioite)
RULES = (RULE,)
