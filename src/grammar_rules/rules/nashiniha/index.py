"""Noun + なしには: without (something essential)."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.nashini_nashideha.index import match_nashini_nashideha


def match_nashiniha(tokens):
    # Reuse the existing antecedent and なしに checks, then include は.
    ends = {end: start for start, end in match_nashini_nashideha(tokens)}
    for i, token in enumerate(tokens):
        if token['span'][1] in ends and next_matches(tokens, i, 'は', '助詞', 'は'):
            yield ends[token['span'][1]], tokens[i + 1]['span'][1]


RULE = GrammarRule(id='nashiniha', expression='〜なしには', reading='なしには',
                   meaning='without; cannot without', match=match_nashiniha, order=1)
RULES = (RULE,)
