"""Alternative examples marked by repeated なり."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_narinari(tokens):
    markers = []
    for i in range(1, len(tokens)):
        before, marker = tokens[i - 1:i + 1]
        pos = before['pos'][:1]
        nominal = pos in (['名詞'], ['代名詞'])
        verb = pos == ['動詞'] and before['surface'] == before['lemma']
        if (marker['surface'] == 'なり' and marker['lemma'] == 'なり'
                and marker['pos'][:1] in (['助詞'], ['接尾辞'])
                and (nominal or verb) and adjacent(before, marker)):
            markers.append(i)
    for first, second in zip(markers, markers[1:]):
        between = tokens[first + 1:second]
        if (not between or between[0]['surface'] == 'の'
                or any(t['surface'] in ('。', '！', '？', '!', '?', '\n') for t in between)):
            continue
        yield tokens[first]['span']


RULE = GrammarRule(order=2, id='narinari', expression='〜なり〜なり',
                   reading='なりなり', meaning='such as ... or ...; either ... or ...',
                   match=match_narinari)
RULES = (RULE,)
