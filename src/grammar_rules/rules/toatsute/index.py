"""Circumstantial reason expressed with とあって."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toatsute(tokens):
    for i in range(1, len(tokens) - 2):
        previous, particle, verb, te = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['surface'] != 'あっ' or verb['lemma'] != 'ある'
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        if (i + 3 < len(tokens) and adjacent(te, tokens[i + 3])
                and tokens[i + 3]['surface'] in ('は', 'も')
                and tokens[i + 3]['pos'][:1] == ['助詞']):
            continue
        yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=75, id='toatsute', expression='〜とあって',
                   reading='とあって', meaning='because of the circumstances; given that',
                   match=match_toatsute)
RULES = (RULE,)
