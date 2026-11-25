"""Noun + を抜きにしても: even without; even setting aside."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.onukinishite.index import match_onukinishite


def match_onukinishitemo(tokens):
    short_matches = {end: start for start, end in match_onukinishite(tokens)}
    for i, token in enumerate(tokens):
        if (token['span'][1] in short_matches
                and next_matches(tokens, i, 'も', '助詞', 'も')):
            yield short_matches[token['span'][1]], tokens[i + 1]['span'][1]


RULE = GrammarRule(
    order=1, id='onukinishitemo', expression='〜を抜きにしても',
    reading='をぬきにしても', meaning='even without; even setting aside',
    match=match_onukinishitemo,
)
RULES = (RULE,)
