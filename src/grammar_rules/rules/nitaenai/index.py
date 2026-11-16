"""Negative に堪える / に耐える construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end

NITAENAI_EXPRESSION = '〜に堪えない'


def match_nitaenai(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb = tokens[i - 1:i + 2]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('堪える', '耐える', 'たえる')
                or verb['surface'] not in ('堪え', '耐え', 'たえ')
                or not adjacent(preceding, ni) or not adjacent(ni, verb)):
            continue
        end = negative_predicate_end(tokens, i + 1, polite_lemma=verb['lemma'],
                                     polite_stems=(verb['surface'],),
                                     plain_lemma=verb['lemma'], plain_stems=(verb['surface'],))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=3, id='nitaenai', expression=NITAENAI_EXPRESSION,
                   reading='にたえない', meaning='cannot bear; deeply feel',
                   match=match_nitaenai)
RULES = (RULE,)
