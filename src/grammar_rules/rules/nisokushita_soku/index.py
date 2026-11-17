"""Noun or pronoun + に即した: in accordance with a standard."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nisokushita(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, ta = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('即し', 'そくし')
                and verb['lemma'] in ('即する', 'そくする', '即す', 'そくす')
                and verb['pos'][:1] == ['動詞']
                and ta['surface'] == 'た' and ta['pos'][:1] == ['助動詞']
                and all(adjacent(a, b) for a, b in ((noun, ni), (ni, verb), (verb, ta)))):
            yield ni['span'][0], ta['span'][1]


RULE = GrammarRule(order=6, id='nisokushita_soku', expression='〜に即した',
                   reading='にそくした', meaning='suited to; in accordance with',
                   match=match_nisokushita)
RULES = (RULE,)
