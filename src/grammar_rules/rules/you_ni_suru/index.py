"""Verb-attached ようにする expressing an effort or habit."""
from src.GrammarConstants import YOU_NI_SURU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_you_ni_suru(tokens):
    for i in range(1, len(tokens)):
        predicate, you = tokens[i - 1:i + 1]
        if you['surface'] not in ('ように', 'よう'):
            continue
        pos = predicate['pos']
        if not adjacent(predicate, you) or len(pos) < 6:
            continue
        if pos[:1] == ['動詞'] and pos[5].startswith(('終止形', '連体形')):
            pass
        elif (predicate['surface'] == 'ない' and predicate['lemma'] == 'ない'
              and pos[:1] == ['助動詞'] and pos[5].startswith(('終止形', '連体形'))
              and i >= 2 and adjacent(tokens[i - 2], predicate)
              and tokens[i - 2]['pos'][:1] == ['動詞']
              and len(tokens[i - 2]['pos']) >= 6
              and tokens[i - 2]['pos'][5].startswith('未然形')):
            pass
        else:
            continue
        if you['surface'] == 'ように' and you['lemma'] == 'ように':
            ni = i
        elif (you['surface'] == 'よう' and you['lemma'] == 'よう'
              and you['pos'][:1] == ['形状詞']
              and next_matches(tokens, i, 'だ', '助動詞', 'に')):
            ni = i + 1
        else:
            continue
        if not next_matches(tokens, ni, 'する', '動詞'):
            continue
        end = positive_verb_end(tokens, ni + 1, lemma='する', base_forms={'する'},
                                past_stems={'し'}, polite_stems={'し'})
        if end is None:
            end = negative_predicate_end(tokens, ni + 1, plain_lemma='する',
                                         plain_stems={'し'}, polite_lemma='する',
                                         polite_stems={'し'})
        if (end is None and tokens[ni + 1]['surface'] == 'し'
                and next_matches(tokens, ni + 1, 'て', '助詞', 'て')):
            end = ni + 2
        if end is not None:
            yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='you_ni_suru', expression=YOU_NI_SURU_EXPRESSION,
                   reading='ようにする', meaning='try to; make a habit of',
                   match=match_you_ni_suru)
