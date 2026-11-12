"""Correction or preference expressed with というより."""
from src.GrammarConstants import TOIUYORI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiuyori(tokens):
    for i in range(1, len(tokens) - 2):
        previous, particle, verb, yori = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] not in ('と', 'って')
                or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or yori['surface'] != 'より' or yori['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        yield particle['span'][0], yori['span'][1]


RULE = GrammarRule(id='toiuyori', expression=TOIUYORI_EXPRESSION,
                   reading='というより', meaning='rather than; more accurately',
                   match=match_toiuyori)
