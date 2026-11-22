"""Distributive ごとに following a noun or attributive verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

GOTONI_EXPRESSION = '〜ごとに'


def match_gotoni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, suffix, particle = tokens[i - 1:i + 2]
        verb = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                and previous['pos'][5].startswith('連体形'))
        if not (previous['pos'][:1] == ['名詞'] or verb):
            continue
        if (suffix['surface'] not in ('ごと', '毎') or suffix['lemma'] not in ('ごと', '毎')
                or suffix['pos'][:2] != ['接尾辞', '名詞的']
                or particle['surface'] != 'に' or particle['lemma'] != 'に'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, suffix) or not adjacent(suffix, particle)):
            continue
        yield suffix['span'][0], particle['span'][1]


RULE = GrammarRule(order=104, id='gotoni', expression=GOTONI_EXPRESSION,
                   reading='ごとに', meaning='each; every', match=match_gotoni)
RULES = (RULE,)
