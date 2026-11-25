"""Noun + を抜きにしては: without; cannot without."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.onukinishite.index import match_onukinishite


def match_onukinishiteha(tokens):
    short_matches = {end: start for start, end in match_onukinishite(tokens)}
    for i, token in enumerate(tokens):
        if (token['span'][1] in short_matches
                and next_matches(tokens, i, 'は', '助詞', 'は')):
            yield short_matches[token['span'][1]], tokens[i + 1]['span'][1]


RULE = GrammarRule(
    order=1, id='onukinishiteha', expression='〜を抜きにしては',
    reading='をぬきにしては', meaning='without; cannot without',
    match=match_onukinishiteha,
)
RULES = (RULE,)
