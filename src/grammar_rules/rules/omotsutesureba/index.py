"""Noun + をもってすれば: given a resource or ability."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.omotsute.index import match_omotsute

OMOTSUTESUREBA_EXPRESSION = '〜をもってすれば'


def match_omotsutesureba(tokens):
    for start, end in match_omotsute(tokens):
        for i, token in enumerate(tokens):
            if token['span'][1] != end:
                continue
            tail = tokens[i + 1:i + 3]
            if (len(tail) == 2
                    and [(t['surface'], t['pos'][0]) for t in tail]
                    == [('すれ', '動詞'), ('ば', '助詞')]
                    and tail[0]['lemma'] == 'する'
                    and all(adjacent(a, b) for a, b in zip([token] + tail, tail))):
                yield start, tail[-1]['span'][1]
            break


RULE = GrammarRule(order=-2, id='omotsutesureba',
                   expression=OMOTSUTESUREBA_EXPRESSION, reading='をもってすれば',
                   meaning='given the ability or resources of; with', match=match_omotsutesureba)
RULES = (RULE,)
