"""Concessive 〜をものともせず (に)."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OMONOTOMOSEZU_EXPRESSION = '〜をものともせず'


def match_omonotomosezu(tokens):
    for i in range(1, len(tokens) - 5):
        previous = tokens[i - 1]
        parts = tokens[i:i + 6]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or not adjacent(previous, parts[0])
                or not all(adjacent(a, b) for a, b in pairwise(parts))):
            continue
        wo, mono, to, mo, se, zu = parts
        if not (wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and mono['surface'] in ('もの', '物') and mono['pos'][:1] == ['名詞']
                and to['surface'] == 'と' and to['pos'][:1] == ['助詞']
                and mo['surface'] == 'も' and mo['pos'][:1] == ['助詞']
                and se['surface'] == 'せ' and se['lemma'] == 'する' and se['pos'][:1] == ['動詞']
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']):
            continue
        end = zu
        if (i + 6 < len(tokens) and adjacent(zu, tokens[i + 6])
                and tokens[i + 6]['surface'] == 'に' and tokens[i + 6]['pos'][:1] == ['助詞']):
            end = tokens[i + 6]
        yield wo['span'][0], end['span'][1]


RULE = GrammarRule(order=1, id='omonotomosezu', expression=OMONOTOMOSEZU_EXPRESSION,
                   reading='をものともせず', meaning='undeterred by; in spite of',
                   match=match_omonotomosezu)
RULES = (RULE,)
