"""Negative predicate + こともない: a qualified affirmation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_naikotomonai(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or not next_matches(tokens, i, 'こと', '名詞', 'こと')
                or not next_matches(tokens, i + 1, 'も', '助詞', 'も')
                or i + 3 >= len(tokens) or not adjacent(tokens[i + 2], tokens[i + 3])):
            continue
        end = negative_predicate_end(tokens, i + 3, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naikotomonai', expression='〜ないこともない',
                   reading='ないこともない', meaning='not impossible; possibly, with reservation',
                   match=match_naikotomonai, order=80)
RULES = (RULE,)
