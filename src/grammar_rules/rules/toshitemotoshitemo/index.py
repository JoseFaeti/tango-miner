"""Paired alternatives marked by としても."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.toshitemo.index import match_toshitemo

TOSHITEMOTOSHITEMO_EXPRESSION = '〜としても〜としても'


def match_toshitemotoshitemo(tokens):
    markers = list(match_toshitemo(tokens))
    for (start, end), (next_start, _) in zip(markers, markers[1:]):
        between = [t for t in tokens if end <= t['span'][0] and t['span'][1] <= next_start]
        if (not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n')
                               for t in between)
                or not any(t['pos'][:1] not in (['補助記号'], ['空白']) for t in between)):
            continue
        yield start, end


RULE = GrammarRule(order=17, id='toshitemotoshitemo', expression=TOSHITEMOTOSHITEMO_EXPRESSION,
                   reading='としてもとしても', meaning='whether ... or ...',
                   match=match_toshitemotoshitemo)

RULES = (RULE,)
