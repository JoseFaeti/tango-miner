"""Conditional circumstances expressed with とあっては."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toatsuteha(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, te, ha = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['surface'] != 'あっ' or verb['lemma'] != 'ある'
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or ha['surface'] != 'は' or ha['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield particle['span'][0], ha['span'][1]


RULE = GrammarRule(order=75, id='toatsuteha', expression='〜とあっては',
                   reading='とあっては', meaning='if circumstances are such that; given that',
                   match=match_toatsuteha)
RULES = (RULE,)
