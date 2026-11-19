"""Additive ばかりか: not only, but also."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

BAKARIKA_EXPRESSION = '〜ばかりか'


def match_bakarika(tokens):
    for i in range(1, len(tokens) - 2):
        previous, marker, ka, following = tokens[i - 1:i + 3]
        if (marker['surface'] == 'ばかり' and marker['lemma'] == 'ばかり'
                and marker['pos'][:2] == ['助詞', '副助詞']
                and ka['surface'] == 'か' and ka['lemma'] == 'か'
                and ka['pos'][:1] == ['助詞']
                and previous['pos'][:1] != ['空白']
                and (previous['pos'][:1] != ['補助記号']
                     or previous['pos'][1:2] == ['括弧閉'])
                and following['pos'][:1] not in (['空白'], ['補助記号'])
                and adjacent(previous, marker) and adjacent(marker, ka)):
            yield marker['span'][0], ka['span'][1]


RULE = GrammarRule(order=1, id='bakarika', expression=BAKARIKA_EXPRESSION,
                   reading='ばかりか', meaning='not only; but also', match=match_bakarika)
RULES = (RULE,)
