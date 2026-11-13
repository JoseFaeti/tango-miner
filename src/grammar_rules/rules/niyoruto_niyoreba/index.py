"""Noun + によると / によれば, with bounded lexical controls."""
from src.GrammarConstants import NIYORUTO_NIYOREBA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_niyoruto_niyoreba(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, ending = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != 'よる' or verb['pos'][:1] != ['動詞']
                or (verb['surface'], ending['surface']) not in (('よる', 'と'), ('よれ', 'ば'))
                or ending['pos'][:1] != ['助詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)
                or not adjacent(verb, ending)):
            continue
        yield ni['span'][0], ending['span'][1]


RULE = GrammarRule(id='niyoruto_niyoreba', expression=NIYORUTO_NIYOREBA_EXPRESSION,
                   reading='によると / によれば', meaning='according to',
                   match=match_niyoruto_niyoreba)
