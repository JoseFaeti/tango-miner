"""Limiting and excessive ばかり and colloquial variants."""
BAKARI_EXPRESSION = '〜ばかり'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tabakari.index import match_tabakari


def match_bakari(tokens):
    recent = set(match_tabakari(tokens))
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('ばかり', 'ばっかり', 'ばっか')
                or marker['lemma'] not in ('ばかり', 'ばっかり', 'ばっか')
                or marker['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, marker)
                or previous['pos'][:1] == ['空白']
                or (previous['pos'][:1] == ['補助記号']
                    and previous['pos'][1:2] != ['括弧閉'])
                or tuple(marker['span']) in recent):
            continue
        if (i + 1 < len(tokens) and adjacent(marker, tokens[i + 1])
                and tokens[i + 1]['surface'] == 'に'
                and tokens[i + 1]['pos'][:1] == ['助詞']):
            continue
        yield tuple(marker['span'])


RULE = GrammarRule(order=57, id='bakari', expression=BAKARI_EXPRESSION,
                   reading='ばかり', meaning='only; nothing but; excessively',
                   match=match_bakari)

RULES = (RULE,)
