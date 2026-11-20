"""Verb + ずもがな: better left undone."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ZUMOGANA_EXPRESSION = '〜ずもがな'


def match_zumogana(tokens):
    for i in range(1, len(tokens) - 2):
        verb, zu, first, second = tokens[i - 1:i + 3]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and (first['surface'], second['surface']) in (('も', 'がな'), ('もが', 'な'))
                and first['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((verb, zu, first), (zu, first, second)))):
            continue
        yield zu['span'][0], second['span'][1]


RULE = GrammarRule(id='zumogana', expression=ZUMOGANA_EXPRESSION,
                   reading='ずもがな', meaning='better left undone; needless to do',
                   match=match_zumogana, order=1)
RULES = (RULE,)
