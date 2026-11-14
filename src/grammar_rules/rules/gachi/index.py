"""Verb stem or noun + がち, a tendency toward a state or action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

GACHI_EXPRESSION = '〜がち'


def match_gachi(tokens):
    for i in range(1, len(tokens)):
        host, suffix = tokens[i - 1:i + 1]
        pos = host['pos']
        valid_host = (pos[:1] == ['名詞'] or
                      (pos[:1] == ['動詞'] and len(pos) >= 6
                       and pos[5].startswith('連用形')))
        if (valid_host and adjacent(host, suffix)
                and suffix['surface'] == suffix['lemma'] == 'がち'
                and suffix['pos'][:2] == ['接尾辞', '形状詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='gachi', expression=GACHI_EXPRESSION,
                   reading='がち', meaning='tend to; prone to', match=match_gachi)
RULES = (RULE,)
