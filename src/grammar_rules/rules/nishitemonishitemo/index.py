"""Paired alternatives marked by にしても."""
from src.GrammarConstants import NISHITEMONISHITEMO_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nishitemo.index import match_nishitemo


def match_nishitemonishitemo(tokens):
    markers = list(match_nishitemo(tokens))
    for (start, end), (next_start, next_end) in zip(markers, markers[1:]):
        between = [t for t in tokens if end <= t['span'][0] and t['span'][1] <= next_start]
        if (not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n')
                               for t in between)
                or not any(t['pos'][:1] not in (['補助記号'], ['空白']) for t in between)):
            continue
        # Reserve only a marker, leaving the alternative available for mining.
        yield start, end


RULE = GrammarRule(id='nishitemonishitemo', expression=NISHITEMONISHITEMO_EXPRESSION,
                   reading='にしてもにしても', meaning='whether ... or ...',
                   match=match_nishitemonishitemo)
