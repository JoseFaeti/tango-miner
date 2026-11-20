"""Verb + ずじまい: end up without doing something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.zu.index import match_zu

ZUJIMAI_EXPRESSION = '〜ずじまい'


def match_zujimai(tokens):
    negatives = {tuple(span) for span in match_zu(tokens)}
    for i in range(1, len(tokens)):
        zu, jimai = tokens[i - 1:i + 1]
        if (tuple(zu['span']) in negatives and adjacent(zu, jimai)
                and ((jimai['surface'] == 'じまい' and jimai['pos'][:1] == ['接尾辞'])
                     or (jimai['surface'] == '仕舞い' and jimai['pos'][:1] == ['名詞']))):
            yield zu['span'][0], jimai['span'][1]


RULE = GrammarRule(id='zujimai', expression=ZUJIMAI_EXPRESSION,
                   reading='ずじまい', meaning='end up without doing',
                   match=match_zujimai, order=1)
RULES = (RULE,)
