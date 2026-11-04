"""A verb followed by ことにする, describing a decision."""
from src.GrammarConstants import KOTO_NI_SURU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


def match_koto_ni_suru(tokens):
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
        if koto['surface'] == 'ことにする' and koto['lemma'] == 'ことにする':
            end = i
        elif (koto['surface'] == 'こと' and koto['lemma'] == 'こと'
              and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'に', '助詞', 'に')
              and next_matches(tokens, i + 1, 'する', '動詞')):
            end = positive_verb_end(tokens, i + 2, lemma='する',
                                    base_forms={'する'}, past_stems={'し'},
                                    polite_stems={'し'})
            if end is None:
                continue
        else:
            continue
        yield koto['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='koto_ni_suru', expression=KOTO_NI_SURU_GRAMMAR_EXPRESSION,
    reading='ことにする', meaning='decide to do; make it a rule to do',
    match=match_koto_ni_suru,
)
