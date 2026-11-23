"""Concessive にせよ / にしろ with evidence beyond a bare noun."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NISEYO_NISHIRO_EXPRESSION = '〜にせよ / 〜にしろ'


def match_niseyo_nishiro(tokens):
    markers = []
    for i in range(1, len(tokens) - 1):
        previous, ni, verb = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('せよ', 'しろ')
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not adjacent(previous, ni) or not adjacent(ni, verb)):
            continue
        markers.append(i)

    paired = set()
    for first, second in pairwise(markers):
        between = tokens[first + 2:second]
        if (any(t['pos'][:1] not in (['補助記号'], ['空白']) for t in between)
                and not any(t['surface'] in ('。', '！', '？', '!', '?', '\n')
                            for t in between)
                and not any('\n' in t['surface'] for t in tokens[first:second])):
            paired.update((first, second))

    for i in markers:
        previous = tokens[i - 1]
        if (i in paired
                or previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                or previous['surface'] in ('何', 'なに', 'なん', 'どちら', 'どっち',
                                           'いずれ', '何れ', 'どう', 'そう')):
            yield tokens[i]['span'][0], tokens[i + 1]['span'][1]


RULE = GrammarRule(order=18, id='niseyo_nishiro', expression=NISEYO_NISHIRO_EXPRESSION,
                   reading='にせよ / にしろ', meaning='even if; whichever',
                   match=match_niseyo_nishiro)

RULES = (RULE,)
