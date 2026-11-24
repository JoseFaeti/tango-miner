"""Noun + に応えて: in response to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKOTAETE_EXPRESSION = '〜に応えて'


def match_nikotaete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('応え', 'こたえ')
                or verb['lemma'] not in ('応える', 'こたえる')
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


RULE = GrammarRule(order=6, id='nikotaete', expression=NIKOTAETE_EXPRESSION,
                   reading='にこたえて', meaning='in response to; to meet expectations',
                   match=match_nikotaete)

RULES = (RULE,)
