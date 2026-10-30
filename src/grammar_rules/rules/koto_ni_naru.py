"""A verb followed by ことになる, describing an outcome or decision."""
from src.GrammarConstants import KOTO_NI_NARU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_koto_ni_naru(tokens):
    for i in range(1, len(tokens)):
        predicate, koto = tokens[i - 1:i + 1]
        pos = predicate['pos']
        if not adjacent(predicate, koto) or len(pos) < 6:
            continue
        if pos[:1] == ['動詞'] and pos[5].startswith(('終止形', '連体形')):
            pass
        elif (predicate['lemma'] == 'ない' and predicate['surface'] == 'ない'
              and pos[:1] == ['助動詞'] and pos[5].startswith(('終止形', '連体形'))
              and i >= 2 and adjacent(tokens[i - 2], predicate)
              and tokens[i - 2]['pos'][:1] == ['動詞']
              and len(tokens[i - 2]['pos']) >= 6
              and tokens[i - 2]['pos'][5].startswith('未然形')):
            pass
        else:
            continue
        if koto['surface'] == 'ことになる' and koto['lemma'] == 'ことになる':
            end = i
        elif (koto['surface'] == 'こと' and koto['lemma'] == 'こと'
              and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'に', '助詞', 'に')
              and next_matches(tokens, i + 1, 'なる', '動詞')):
            end = positive_verb_end(tokens, i + 2, lemma='なる',
                                    base_forms={'なる'}, past_stems={'なっ'},
                                    polite_stems={'なり'})
            if end is None:
                continue
        else:
            continue
        yield koto['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='koto_ni_naru', expression=KOTO_NI_NARU_GRAMMAR_EXPRESSION,
    reading='ことになる', meaning='it is decided that; it turns out that; end up doing',
    match=match_koto_ni_naru,
)
