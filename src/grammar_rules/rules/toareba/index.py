"""Circumstantial conditional expressed with とあれば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toareba(tokens):
    for i in range(1, len(tokens) - 2):
        previous, particle, verb, ba = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['surface'] != 'あれ' or verb['lemma'] != 'ある'
                or verb['pos'][:1] != ['動詞']
                or ba['surface'] != 'ば' or ba['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        yield particle['span'][0], ba['span'][1]


RULE = GrammarRule(order=75, id='toareba', expression='〜とあれば',
                   reading='とあれば', meaning='if the circumstances call for it; if it is for',
                   match=match_toareba)
RULES = (RULE,)
