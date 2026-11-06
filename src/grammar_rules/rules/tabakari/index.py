"""Recently completed actions expressed by verb + たばかり."""
from src.GrammarConstants import TABAKARI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tabakari(tokens):
    for i in range(2, len(tokens)):
        verb, past, marker = tokens[i - 2:i + 1]
        if marker['surface'] not in ('ばかり', 'ばっかり', 'ばっか'):
            continue
        if (verb['pos'][:1] != ['動詞']
                or past['surface'] not in ('た', 'だ')
                or past['lemma'] != past['surface']
                or past['pos'][:1] != ['助動詞']
                or marker['surface'] not in ('ばかり', 'ばっかり', 'ばっか')
                or marker['lemma'] not in ('ばかり', 'ばっかり', 'ばっか')
                or marker['pos'][:1] != ['助詞']
                or not adjacent(verb, past) or not adjacent(past, marker)):
            continue
        if (i + 1 < len(tokens) and adjacent(marker, tokens[i + 1])
                and tokens[i + 1]['surface'] == 'に'
                and tokens[i + 1]['pos'][:1] == ['助詞']):
            continue
        yield marker['span'][0], marker['span'][1]


RULE = GrammarRule(id='tabakari', expression=TABAKARI_EXPRESSION,
                   reading='たばかり', meaning='just did; recently completed',
                   match=match_tabakari)
