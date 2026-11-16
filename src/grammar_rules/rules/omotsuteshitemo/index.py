"""Noun + をもってしても: even with a particular resource or ability."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.omotsute.index import match_omotsute

OMOTSUTESHITEMO_EXPRESSION = '〜をもってしても'


def match_omotsuteshitemo(tokens):
    for start, end in match_omotsute(tokens):
        for i, token in enumerate(tokens):
            if token['span'][1] != end:
                continue
            tail = tokens[i + 1:i + 4]
            if (len(tail) == 3
                    and [(t['surface'], t['pos'][0]) for t in tail]
                    == [('し', '動詞'), ('て', '助詞'), ('も', '助詞')]
                    and tail[0]['lemma'] == 'する'
                    and all(adjacent(a, b) for a, b in zip([token] + tail, tail))):
                yield start, tail[-1]['span'][1]
            break


RULE = GrammarRule(order=-2, id='omotsuteshitemo',
                   expression=OMOTSUTESHITEMO_EXPRESSION, reading='をもってしても',
                   meaning='even with; even using', match=match_omotsuteshitemo)
RULES = (RULE,)
