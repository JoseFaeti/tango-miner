"""Negative predicate + ものでもない: a qualified affirmation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_naimonodemonai(tokens):
    for i, negative in enumerate(tokens):
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or not next_matches(tokens, i, 'もの', '名詞', 'もの')
                or i + 2 >= len(tokens) or not adjacent(tokens[i + 1], tokens[i + 2])
                or tokens[i + 2]['surface'] != 'で'):
            continue
        copula = tokens[i + 2]
        if ((copula['lemma'], copula['pos'][:1]) not in
                (('で', ['助詞']), ('だ', ['助動詞']))
                or not next_matches(tokens, i + 2, 'も', '助詞', 'も')
                or i + 4 >= len(tokens) or not adjacent(tokens[i + 3], tokens[i + 4])):
            continue
        end = negative_predicate_end(tokens, i + 4, polite_lemma='ある', polite_stems={'あり'})
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naimonodemonai', expression='〜ないものでもない',
                   reading='ないものでもない', meaning='not impossible; not entirely out of the question',
                   match=match_naimonodemonai, order=80)
RULES = (RULE,)
