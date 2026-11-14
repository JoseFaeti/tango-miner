"""Verb irrealis + ずに: without doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ZUNI_EXPRESSION = '〜ずに'


def match_zuni(tokens):
    for i in range(1, len(tokens) - 1):
        verb, zu, ni = tokens[i - 1:i + 2]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and adjacent(verb, zu) and adjacent(zu, ni)):
            yield zu['span'][0], ni['span'][1]


RULE = GrammarRule(id='zuni', expression=ZUNI_EXPRESSION, reading='ずに',
                   meaning='without doing', match=match_zuni, order=1)
RULES = (RULE,)
