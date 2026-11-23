"""Literary verbal negative followed by して: without doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.zu.index import match_zu

EXPRESSION = '〜ずして'


def match_zushite(tokens):
    negative_spans = {tuple(span) for span in match_zu(tokens)}
    for i, token in enumerate(tokens[:-2]):
        if (tuple(token['span']) in negative_spans
                and next_matches(tokens, i, 'する', '動詞', 'し')
                and next_matches(tokens, i + 1, 'て', '助詞', 'て')):
            yield token['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='zushite', expression=EXPRESSION, reading='ずして',
                   meaning='without doing (formal)', match=match_zushite, order=1)
RULES = (RULE,)
