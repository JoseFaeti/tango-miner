"""Repeated concessive alternatives marked by にせよ or にしろ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.niseyo_nishiro.index import match_niseyo_nishiro


def match_niseyoniseyo(tokens):
    markers = list(match_niseyo_nishiro(tokens))
    for first, second in zip(markers, markers[1:]):
        between = [t for t in tokens if first[1] <= t['span'][0] and t['span'][1] <= second[0]]
        if (not between or not any(t['pos'][:1] not in (['補助記号'], ['空白']) for t in between)
                or any(t['surface'] in ('。', '！', '？', '!', '?', '\n') for t in between)):
            continue
        yield first


RULE = GrammarRule(order=18, id='niseyoniseyo', expression='〜にせよ〜にせよ',
                   reading='にせよにせよ', meaning='whether ... or ...',
                   match=match_niseyoniseyo)
RULES = (RULE,)
