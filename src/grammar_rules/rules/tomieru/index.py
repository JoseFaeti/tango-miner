"""Predicate-attached と見える expressing an inference."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    negative_predicate_end,
    next_matches,
    positive_verb_end,
)


def match_tomieru(tokens):
    for i in range(1, len(tokens)):
        previous, to = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not (adjacent(previous, to) and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))
                and to['surface'] == 'と' and to['pos'][:2] == ['助詞', '格助詞']
                and next_matches(tokens, i, '見える', '動詞')):
            continue
        end = positive_verb_end(tokens, i + 1, lemma='見える', base_forms={'見える'},
                                past_stems={'見え'}, polite_stems={'見え'}, te_stems={'見え'})
        if end is None:
            end = negative_predicate_end(tokens, i + 1, plain_lemma='見える',
                                         plain_stems={'見え'}, polite_lemma='見える',
                                         polite_stems={'見え'})
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='tomieru', expression='〜と見える',
                   reading='とみえる', meaning='it seems that; apparently', match=match_tomieru)
RULES = (RULE,)
