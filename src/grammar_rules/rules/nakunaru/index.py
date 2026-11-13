"""Verbal negative followed by なる, expressing cessation."""
from src.GrammarConstants import NAKUNARU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_nakunaru(tokens):
    for i in range(1, len(tokens) - 1):
        previous, negative = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (not adjacent(previous, negative) or len(pos) < 6
                or not pos[5].startswith('未然形')
                or not (pos[:1] == ['動詞'] or (pos[:1] == ['助動詞']
                        and previous['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                or negative['surface'] != 'なく' or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or not next_matches(tokens, i, 'なる', '動詞')):
            continue
        end = positive_verb_end(tokens, i + 1, lemma='なる', base_forms={'なる'},
                                past_stems={'なっ'}, polite_stems={'なり'}, te_stems={'なっ'})
        if end is None:
            end = negative_predicate_end(tokens, i + 1, plain_lemma='なる',
                                         plain_stems={'なら'}, polite_lemma='なる',
                                         polite_stems={'なり'})
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nakunaru', expression=NAKUNARU_EXPRESSION,
                   reading='なくなる', meaning='stop doing; cease to', match=match_nakunaru)
