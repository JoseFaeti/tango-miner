"""A literary negative state continuing unchanged: ぬまま."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.mama.index import match_mama
from src.grammar_rules.rules.nu_n.index import match_nu_n

NUMAMA_EXPRESSION = '〜ぬまま'


def match_numama(tokens):
    negative_spans = {tuple(span) for span in match_nu_n(tokens)}
    mama_spans = set(match_mama(tokens))
    for negative, marker in zip(tokens, tokens[1:]):
        if (negative['surface'] == 'ぬ' and tuple(negative['span']) in negative_spans
                and tuple(marker['span']) in mama_spans and adjacent(negative, marker)):
            yield negative['span'][0], marker['span'][1]


RULE = GrammarRule(order=100, id='numama', expression=NUMAMA_EXPRESSION,
                   reading='ぬまま', meaning='without doing; remaining in a negative state',
                   match=match_numama)
RULES = (RULE,)
