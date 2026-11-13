"""Noun + に限って: only, or unexpectedly in this particular case."""
from src.GrammarConstants import NIKAGITTSUTE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikagitsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('限っ', 'かぎっ')
                and verb['lemma'] in ('限る', 'かぎる') and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(id='nikagitsute', expression=NIKAGITTSUTE_EXPRESSION,
                   reading='にかぎって', meaning='only; particularly (contrary to expectation)',
                   match=match_nikagitsute)
