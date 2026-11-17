"""Noun + を兼ねて: serving also as another purpose or role."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


OKANETE_EXPRESSION = '〜を兼ねて'


def match_okanete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('兼ね', 'かね')
                or verb['lemma'] not in ('兼ねる', 'かねる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, particle) or not adjacent(particle, verb)):
            continue
        for suffix in ((('て', '助詞'),), (('まし', '助動詞'), ('て', '助詞'))):
            tail = tokens[i + 2:i + 2 + len(suffix)]
            if (len(tail) == len(suffix)
                    and [(t['surface'], t['pos'][0]) for t in tail] == list(suffix)
                    and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
                yield particle['span'][0], tail[-1]['span'][1]
                break


RULE = GrammarRule(order=7, id='okanete', expression=OKANETE_EXPRESSION,
                   reading='をかねて', meaning='also serving as; doubling as',
                   match=match_okanete)

RULES = (RULE,)
