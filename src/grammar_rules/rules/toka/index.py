"""Clause-attached とか for quotation or uncertain reports."""
TOKA_EXPRESSION = '〜とか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toka(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, particle, ending = tokens[i - 1:i + 2]
        if (predicate['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not adjacent(predicate, particle) or not adjacent(particle, ending)):
            continue
        if (particle['surface'] == 'と' and particle['lemma'] == 'と'
                and particle['pos'][:2] == ['助詞', '格助詞']
                and ending['surface'] == 'か' and ending['lemma'] == 'か'
                and ending['pos'][:2] == ['助詞', '副助詞']):
            yield particle['span'][0], ending['span'][1]


RULE = GrammarRule(order=28, id='toka', expression=TOKA_EXPRESSION, reading='とか',
                   meaning='(they say) that; reportedly', match=match_toka)

RULES = (RULE,)
