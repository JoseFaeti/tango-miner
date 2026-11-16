"""Negative expression of worth or sufficiency."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_nitarinai(tokens):
    for i in range(1, len(tokens) - 1):
        before, particle, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['動詞'])
                or particle['surface'] != 'に' or particle['pos'][:1] != ['助詞']
                or not adjacent(before, particle) or not adjacent(particle, verb)):
            continue
        end = negative_predicate_end(tokens, i + 1, plain_lemma='足りる',
                                     plain_stems=('足り', 'たり'),
                                     polite_lemma='足りる', polite_stems=('足り', 'たり'),
                                     continuative=True, extended=True)
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nitarinai', expression='〜に足りない', reading='にたりない',
                   meaning='not worth; insufficient for', match=match_nitarinai)
RULES = (RULE,)
