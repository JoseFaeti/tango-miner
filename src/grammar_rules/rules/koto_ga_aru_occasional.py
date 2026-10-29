"""Occasional occurrence supported by a nearby frequency cue."""
from src.GrammarConstants import KOTO_GA_ARU_OCCASIONAL
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end


RULE = GrammarRule(
    id='koto_ga_aru_occasional', expression=KOTO_GA_ARU_OCCASIONAL,
    reading='ことがある',
    meaning='Grammar: a non-past verb + ことがある can describe an occasional occurrence.',
    match=lambda tokens: match_occasional(tokens),
)


CUES = {'時々', 'ときどき', 'たまに', '偶に', '時には', 'ときには'}
# A sentence boundary or quotation boundary ends the evidence window. Commas
# are allowed, as in the observed sentence beginning 時には、.
BOUNDARIES = set('。！？!?「」『』；;')


def has_frequency_cue(tokens, verb_index):
    for token in reversed(tokens[max(0, verb_index - 20):verb_index]):
        if any(char in BOUNDARIES for char in token['surface']):
            return False
        if token['surface'] in CUES:
            return True
    return False


def match_occasional(tokens):
    for i in range(1, len(tokens)):
        verb, koto = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, koto) or not has_frequency_cue(tokens, i - 1)):
            continue
        if koto['surface'] == 'ことがある' and koto['lemma'] == 'ことがある':
            end = i
        elif (koto['surface'] == 'こと' and koto['pos'][:1] == ['名詞']
              and next_matches(tokens, i, 'が', '助詞', 'が')
              and next_matches(tokens, i + 1, 'ある', '動詞')):
            end = positive_verb_end(tokens, i + 2, lemma='ある',
                                    base_forms={'ある'}, past_stems={'あっ'},
                                    polite_stems={'あり'})
            if end is None:
                continue
        else:
            continue
        yield koto['span'][0], tokens[end]['span'][1]
