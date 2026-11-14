"""Negative predicate + でもない: a qualified affirmation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_naidemonai(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or i + 1 >= len(tokens) or not adjacent(negative, tokens[i + 1])
                or tokens[i + 1]['surface'] != 'で'
                or (tokens[i + 1]['lemma'], tokens[i + 1]['pos'][:1]) not in
                   (('で', ['助詞']), ('だ', ['助動詞']))
                or not next_matches(tokens, i + 1, 'も', '助詞', 'も')
                or i + 3 >= len(tokens) or not adjacent(tokens[i + 2], tokens[i + 3])):
            continue
        end = negative_predicate_end(tokens, i + 3, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naidemonai', expression='〜ないでもない',
                   reading='ないでもない', meaning='not impossible; not entirely untrue',
                   match=match_naidemonai, order=80)
RULES = (RULE,)
