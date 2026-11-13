"""Unexpected result relative to a standard expressed with にしては."""
from src.GrammarConstants import NISHITEHA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishiteha(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ni, shi, te, ha = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or [(t['surface'], t['pos'][:1]) for t in (ni, shi, te, ha)] !=
                [('に', ['助詞']), ('し', ['動詞']), ('て', ['助詞']), ('は', ['助詞'])]
                or shi['lemma'] != 'する'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield ni['span'][0], ha['span'][1]


RULE = GrammarRule(id='nishiteha', expression=NISHITEHA_EXPRESSION,
                   reading='にしては', meaning='considering; for (something unexpected)',
                   match=match_nishiteha)
