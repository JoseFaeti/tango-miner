"""Concessive circumstances expressed with とあっても."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toatsutemo(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, te, mo = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['surface'] != 'あっ' or verb['lemma'] != 'ある'
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield particle['span'][0], mo['span'][1]


RULE = GrammarRule(order=75, id='toatsutemo', expression='〜とあっても',
                   reading='とあっても', meaning='even if circumstances are such that',
                   match=match_toatsutemo)
RULES = (RULE,)
