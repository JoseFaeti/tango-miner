"""Unexpected behavior from someone of a particular standing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOMOAROUMONOGA_EXPRESSION = '〜ともあろう者が'


def match_tomoaroumonoga(tokens):
    for i in range(1, len(tokens) - 4):
        previous, to, mo, arou, mono, ga = tokens[i - 1:i + 5]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or arou['surface'] != 'あろう' or arou['lemma'] != 'ある'
                or arou['pos'][:1] != ['動詞']
                or mono['surface'] not in ('者', 'もの') or mono['pos'][:1] != ['名詞']
                or ga['surface'] != 'が' or ga['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        yield to['span'][0], ga['span'][1]


RULE = GrammarRule(order=13, id='tomoaroumonoga', expression=TOMOAROUMONOGA_EXPRESSION,
                   reading='ともあろうものが',
                   meaning='someone of that standing, unexpectedly or disappointingly',
                   match=match_tomoaroumonoga)

RULES = (RULE,)
