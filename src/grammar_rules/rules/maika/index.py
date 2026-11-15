"""Tentative negative conjecture or suggestion with verbal まいか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.mai.index import match_mai
from src.grammar_rules.rules.dehaarumaika.index import match_dehaarumaika

MAIKA_EXPRESSION = '〜まいか'


def match_maika(tokens):
    mai_spans = list(match_mai(tokens))
    specific = list(match_dehaarumaika(tokens))
    for i, token in enumerate(tokens):
        if (token['span'] in mai_spans
                and next_matches(tokens, i, 'か', '助詞', 'か')
                and tokens[i + 1]['pos'][:2] == ['助詞', '終助詞']):
            start, end = token['span'][0], tokens[i + 1]['span'][1]
            if not any(a <= start and end <= b for a, b in specific):
                yield start, end


RULE = GrammarRule(id='maika', expression=MAIKA_EXPRESSION, reading='まいか',
                   meaning='might not; perhaps; shall we not', match=match_maika, order=155)
RULES = (RULE,)
