"""Cued habitual non-occurrence, distinct from past experience."""
from src.GrammarConstants import KOTO_GA_NAI
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


RULE = GrammarRule(id='koto_ga_nai', expression=KOTO_GA_NAI,
                   reading='ことがない', meaning='never do; never happens',
                   match=lambda tokens: match_never(tokens))

BOUNDARIES = set('。！？!?「」『』；;')
CUES = {'決して', 'けっして', '絶対に', 'ぜったいに'}


def has_cue(tokens, index):
    for token in reversed(tokens[max(0, index - 20):index]):
        if any(char in BOUNDARIES for char in token['surface']):
            return False
        if token['surface'] in CUES:
            return True
    return False


def match_never(tokens):
    for i in range(1, len(tokens)):
        verb, koto = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, koto) or not has_cue(tokens, i - 1)):
            continue
        if (koto['surface'] == 'ことがない' and koto['lemma'] == 'ことがない'
                and koto['pos'][:1] == ['形容詞']):
            end = i
        elif (koto['surface'] == 'こと' and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'が', '助詞', 'が')
              and i + 2 < len(tokens) and adjacent(tokens[i + 1], tokens[i + 2])):
            end = negative_predicate_end(tokens, i + 2, polite_lemma='ある',
                                         polite_stems={'あり'})
            if end is None:
                continue
        else:
            continue
        yield koto['span'][0], tokens[end]['span'][1]
