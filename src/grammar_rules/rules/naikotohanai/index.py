"""Negative predicate + ことはない: a qualified affirmation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_naikotohanai(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or not next_matches(tokens, i, 'こと', '名詞', 'こと')
                or not next_matches(tokens, i + 1, 'は', '助詞', 'は')
                or i + 3 >= len(tokens) or not adjacent(tokens[i + 2], tokens[i + 3])):
            continue
        end = negative_predicate_end(tokens, i + 3, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naikotohanai', expression='〜ないことはない',
                   reading='ないことはない', meaning='not impossible; not entirely untrue',
                   match=match_naikotohanai, order=80)
RULES = (RULE,)
