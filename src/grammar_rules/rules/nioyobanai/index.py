"""Negative 及ぶ following に: no need or falling short."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end


def match_nioyobanai(tokens):
    for i in range(1, len(tokens) - 1):
        before, particle, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or particle['surface'] != 'に' or particle['pos'][:1] not in (['助詞'], ['助動詞'])
                or verb['lemma'] not in ('及ぶ', 'およぶ')
                or not adjacent(before, particle) or not adjacent(particle, verb)):
            continue
        end = negative_predicate_end(tokens, i + 1, plain_lemma=verb['lemma'],
                                     plain_stems=('及ば', 'およば'),
                                     polite_lemma=verb['lemma'], polite_stems=('及び', 'および'),
                                     continuative=True, extended=True)
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nioyobanai', expression='〜に及ばない', reading='におよばない',
                   meaning='there is no need to; not reach or be equal to',
                   match=match_nioyobanai, order=30)
RULES = (RULE,)
