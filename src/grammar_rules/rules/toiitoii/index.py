"""Paired examples marked by といい."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiitoii(tokens):
    markers = []
    for i in range(1, len(tokens) - 1):
        before, particle, word = tokens[i - 1:i + 2]
        marker = ((word['surface'] == 'いい' and word['pos'][:1] == ['形容詞'])
                  or (word['surface'] == '言い' and word['lemma'] in ('言う', 'いう')
                      and word['pos'][:1] == ['動詞']))
        if (before['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'と' and particle['pos'][:2] == ['助詞', '格助詞']
                and marker and adjacent(before, particle) and adjacent(particle, word)):
            markers.append(i)
    for first, second in itertools.pairwise(markers):
        between = tokens[first + 2:second]
        if not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n') for t in between):
            continue
        yield tokens[first]['span'][0], tokens[first + 1]['span'][1]


RULE = GrammarRule(order=2, id='toiitoii', expression='〜といい〜といい',
                   reading='といいといい', meaning='both ... and ...; whether ... or ...',
                   match=match_toiitoii)
RULES = (RULE,)
