"""Paired examples marked by といわず."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiwazutoiwazu(tokens):
    markers = []
    for i in range(1, len(tokens) - 2):
        before, particle, verb, negative = tokens[i - 1:i + 3]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'と' or particle['pos'][:2] != ['助詞', '格助詞']
                or verb['surface'] not in ('いわ', '言わ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or negative['surface'] != 'ず' or negative['lemma'] != 'ず'
                or negative['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in
                           ((before, particle), (particle, verb), (verb, negative)))):
            continue
        markers.append((i, i + 2))
    for (first, end), (second, second_end) in pairwise(markers):
        between = tokens[end + 1:second]
        if (not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n')
                               for t in between)):
            continue
        # Keep the intervening example available for vocabulary mining.
        yield tokens[first]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='toiwazutoiwazu', expression='〜といわず〜といわず',
                   reading='といわずといわず', meaning='both ... and ...; throughout',
                   match=match_toiwazutoiwazu)
RULES = (RULE,)
