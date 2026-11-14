"""Appearance expressed by the adjective-like suffix げ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

GE_EXPRESSION = '〜げ'


def match_ge(tokens):
    for i in range(1, len(tokens)):
        host, suffix = tokens[i - 1:i + 1]
        pos = host['pos']
        adjective = (pos[:1] == ['形容詞'] and len(pos) >= 6
                     and pos[5].startswith('語幹'))
        nominal = (pos[:1] == ['形状詞'] or
                   (pos[:1] == ['名詞'] and len(pos) >= 3 and pos[2] == '形状詞可能'))
        ari = host['surface'] == 'あり' and host['lemma'] == 'ある' and pos[:1] == ['動詞']
        if ((adjective or nominal or ari) and adjacent(host, suffix)
                and suffix['surface'] == suffix['lemma'] == 'げ'
                and suffix['pos'][:2] == ['接尾辞', '形状詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='ge', expression=GE_EXPRESSION,
                   reading='げ', meaning='looking or seeming; showing signs of', match=match_ge)
RULES = (RULE,)
