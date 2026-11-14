"""A negative state continuing unchanged: ないまま."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.mama.index import match_mama

NAIMAMA_EXPRESSION = '〜ないまま'


def match_naimama(tokens):
    mama_spans = set(match_mama(tokens))
    for negative, marker in zip(tokens, tokens[1:]):
        if (negative['surface'] == 'ない' and negative['lemma'] == 'ない'
                and negative['pos'][:1] in (['助動詞'], ['形容詞'])
                and adjacent(negative, marker) and tuple(marker['span']) in mama_spans):
            yield negative['span'][0], marker['span'][1]


RULE = GrammarRule(order=100, id='naimama', expression=NAIMAMA_EXPRESSION,
                   reading='ないまま', meaning='without doing; remaining in a negative state',
                   match=match_naimama)
RULES = (RULE,)
