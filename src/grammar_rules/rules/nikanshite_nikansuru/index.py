"""Noun + に関して / に関する: concerning a topic."""
from src.GrammarConstants import NIKANSHITE_NIKANSURU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikanshite_nikansuru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != '関する' or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] == '関する':
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] == '関し':
            for suffix in ((('て', '助詞'),), (('まし', '助動詞'), ('て', '助詞'))):
                tail = tokens[i + 2:i + 2 + len(suffix)]
                if (len(tail) == len(suffix)
                        and [(t['surface'], t['pos'][0]) for t in tail] == list(suffix)
                        and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
                    yield ni['span'][0], tail[-1]['span'][1]
                    break


RULE = GrammarRule(id='nikanshite_nikansuru', expression=NIKANSHITE_NIKANSURU_EXPRESSION,
                   reading='にかんして / にかんする', meaning='about; concerning',
                   match=match_nikanshite_nikansuru)
