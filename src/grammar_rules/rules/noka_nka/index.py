"""Explanatory questions and realizations with のか or んか."""
NOKA_NKA_EXPRESSION = '〜のか / 〜んか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_noka_nka(tokens):
    for i in range(1, len(tokens) - 1):
        predicate, marker, ka = tokens[i - 1:i + 2]
        pos = predicate['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or marker['surface'] not in ('の', 'ん')
                or marker['lemma'] != marker['surface']
                or marker['pos'][:2] != ['助詞', '準体助詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] not in (['助詞', '終助詞'], ['助詞', '副助詞'])
                or not adjacent(predicate, marker) or not adjacent(marker, ka)):
            continue
        yield marker['span'][0], ka['span'][1]


RULE = GrammarRule(order=60, id='noka_nka', expression=NOKA_NKA_EXPRESSION,
                   reading='のか / んか', meaning='explanatory question; realization',
                   match=match_noka_nka)

RULES = (RULE,)
