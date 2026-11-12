"""Predicate-attached ように見える expressing an appearance."""
from src.GrammarConstants import YOU_NI_MIERU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_you_ni_mieru(tokens):
    for i in range(1, len(tokens)):
        previous, you = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not adjacent(previous, you):
            continue
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        noun = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                and pos[:1] == ['助詞'] and i >= 2
                and adjacent(tokens[i - 2], previous)
                and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞']))
        if not (predicate or noun):
            continue
        if you['surface'] == 'ように' and you['lemma'] == 'ように':
            ni = i
        elif (you['surface'] == 'よう' and you['lemma'] == 'よう'
              and you['pos'][:1] == ['形状詞']
              and next_matches(tokens, i, 'だ', '助動詞', 'に')):
            ni = i + 1
        else:
            continue
        if not next_matches(tokens, ni, '見える', '動詞'):
            continue
        end = positive_verb_end(tokens, ni + 1, lemma='見える', base_forms={'見える'},
                                past_stems={'見え'}, polite_stems={'見え'}, te_stems={'見え'})
        if end is None:
            end = negative_predicate_end(tokens, ni + 1, plain_lemma='見える',
                                         plain_stems={'見え'}, polite_lemma='見える',
                                         polite_stems={'見え'})
        if end is not None:
            yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='you_ni_mieru', expression=YOU_NI_MIERU_EXPRESSION,
                   reading='ようにみえる', meaning='looks as if; appears to be',
                   match=match_you_ni_mieru)
