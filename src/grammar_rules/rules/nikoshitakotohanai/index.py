"""Nothing is better than the preceding state or action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_nikoshitakotohanai(tokens):
    for i in range(1, len(tokens) - 5):
        ni = tokens[i]
        if (not adjacent(tokens[i - 1], ni)
                or tokens[i - 1]['pos'][:1] not in
                (['動詞'], ['形容詞'], ['形状詞'], ['助動詞'], ['名詞'])
                or ni['surface'] != 'に'
                or (ni['lemma'], ni['pos'][:1]) not in
                (('に', ['助詞']), ('だ', ['助動詞']))
                or not next_matches(tokens, i, '越す', '動詞', '越し')
                or not next_matches(tokens, i + 1, 'た', '助動詞', 'た')
                or not next_matches(tokens, i + 2, 'こと', '名詞', 'こと')
                or not next_matches(tokens, i + 3, 'は', '助詞', 'は')
                or i + 5 >= len(tokens) or not adjacent(tokens[i + 4], tokens[i + 5])):
            continue
        end = negative_predicate_end(tokens, i + 5, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nikoshitakotohanai', expression='〜に越したことはない',
                   reading='にこしたことはない', meaning='nothing is better than; it is best to',
                   match=match_nikoshitakotohanai, order=79)
RULES = (RULE,)
