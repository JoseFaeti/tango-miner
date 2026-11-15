"""Conversational kedo with a following sentence particle."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.kedo_keredomo.index import match_kedo


def match_kedone_kedosa(tokens):
    spans = set(match_kedo(tokens))
    for i in range(len(tokens) - 1):
        marker, particle = tokens[i:i + 2]
        if (tuple(marker['span']) in spans and marker['surface'] == 'けど'
                and adjacent(marker, particle)
                and particle['surface'] in ('ね', 'さ')
                and particle['pos'][:2] == ['助詞', '終助詞']):
            yield marker['span'][0], particle['span'][1]


RULE = GrammarRule(order=107, id='kedone_kedosa', expression='〜けどね / 〜けどさ',
                   reading='けどね / けどさ', meaning='but, you know; though',
                   match=match_kedone_kedosa)
RULES = (RULE,)
