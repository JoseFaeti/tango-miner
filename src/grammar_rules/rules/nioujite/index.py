"""Noun + に応じて: depending on or in accordance with."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIOUJITE_EXPRESSION = '〜に応じて'


def match_nioujite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('応じ', 'おうじ')
                or verb['lemma'] not in ('応じる', '応ずる', 'おうじる', 'おうずる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        for suffix in ((('て', '助詞'),), (('まし', '助動詞'), ('て', '助詞'))):
            tail = tokens[i + 2:i + 2 + len(suffix)]
            if (len(tail) == len(suffix)
                    and [(t['surface'], t['pos'][0]) for t in tail] == list(suffix)
                    and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
                yield ni['span'][0], tail[-1]['span'][1]
                break


RULE = GrammarRule(order=6, id='nioujite', expression=NIOUJITE_EXPRESSION,
                   reading='におうじて', meaning='depending on; in accordance with',
                   match=match_nioujite)

RULES = (RULE,)
