"""Noun + なくしては: without (formal)."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.nakushite.index import match_nakushite


def match_nakushiteha(tokens):
    ends = {end: start for start, end in match_nakushite(tokens)}
    for i, token in enumerate(tokens):
        if token['span'][1] in ends and next_matches(tokens, i, 'は', '助詞', 'は'):
            yield ends[token['span'][1]], tokens[i + 1]['span'][1]


RULE = GrammarRule(id='nakushiteha', expression='〜なくしては', reading='なくしては',
                   meaning='without; cannot without (formal)', match=match_nakushiteha, order=1)
RULES = (RULE,)
