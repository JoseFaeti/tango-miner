"""Verb irrealis + ずとも: even without doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜ずとも'


def match_zutomo(tokens):
    for i in range(1, len(tokens) - 1):
        verb, zu, tomo = tokens[i - 1:i + 2]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and tomo['surface'] == 'とも' and tomo['pos'][:1] == ['助詞']
                and adjacent(verb, zu) and adjacent(zu, tomo)):
            yield zu['span'][0], tomo['span'][1]


RULE = GrammarRule(id='zutomo', expression=EXPRESSION, reading='ずとも',
                   meaning='even without doing', match=match_zutomo, order=1)
RULES = (RULE,)
