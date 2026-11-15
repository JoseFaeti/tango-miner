"""Noun + に先立って: prior to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜に先立って'


def match_nisakidatte(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and (verb['surface'], verb['lemma']) in (
                    ('先立っ', '先立つ'), ('さきだっ', 'さきだつ'))
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip(
                    (noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='nisakidatte', expression=EXPRESSION,
                   reading='にさきだって', meaning='prior to; before',
                   match=match_nisakidatte)
RULES = (RULE,)
