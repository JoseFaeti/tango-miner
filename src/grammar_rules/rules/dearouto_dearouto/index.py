"""Repeated formal concessive alternatives."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.deare_dearouto.index import match_deare_dearouto


def match_dearouto_dearouto(tokens):
    endings = [(start, end) for start, end in match_deare_dearouto(tokens)
               if any(t['span'][0] == start and t['surface'] == 'で' for t in tokens)
               and any(t['span'][1] == end and t['surface'] == 'と' for t in tokens)]
    for (start, end), (next_start, next_end) in itertools.pairwise(endings):
        between = [t for t in tokens if end <= t['span'][0] and t['span'][1] <= next_start]
        if (any(t['pos'][:1] not in (['補助記号'], ['空白']) for t in between)
                and not any(t['surface'] in ('。', '！', '？', '!', '?', '；', ';') for t in between)):
            yield start, end
            yield next_start, next_end


RULE = GrammarRule(order=1, id='dearouto_dearouto', expression='〜であろうと〜であろうと',
                   reading='であろうとであろうと', meaning='whether ... or ...',
                   match=match_dearouto_dearouto)
RULES = (RULE,)
