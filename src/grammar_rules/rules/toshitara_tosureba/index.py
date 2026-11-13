"""Conditional supposition with としたら or とすれば."""
TOSHITARA_TOSUREBA_EXPRESSION = '〜としたら / 〜とすれば'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toshitara_tosureba(tokens):
    for i in range(1, len(tokens) - 2):
        previous, to, verb, ending = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or (verb['surface'], ending['surface'], ending['pos'][:1]) not in
                   (('し', 'たら', ['助動詞']), ('すれ', 'ば', ['助詞']))
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        yield to['span'][0], ending['span'][1]


RULE = GrammarRule(order=14, id='toshitara_tosureba', expression=TOSHITARA_TOSUREBA_EXPRESSION,
                   reading='としたら / とすれば', meaning='if one supposes; assuming that',
                   match=match_toshitara_tosureba)

RULES = (RULE,)
