"""Noun or verb stem + がてら, doing something along the way."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

GATERA_EXPRESSION = '〜がてら'


def match_gatera(tokens):
    for i in range(1, len(tokens)):
        host, suffix = tokens[i - 1:i + 1]
        pos = host['pos']
        valid_host = (pos[:1] == ['名詞'] or
                      (pos[:1] == ['動詞'] and len(pos) >= 6
                       and pos[5].startswith('連用形')))
        if (valid_host and adjacent(host, suffix)
                and suffix['surface'] == suffix['lemma'] == 'がてら'
                and suffix['pos'][:2] == ['接尾辞', '名詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='gatera', expression=GATERA_EXPRESSION,
                   reading='がてら', meaning='while also doing; along the way',
                   match=match_gatera)
RULES = (RULE,)
