"""Colloquial quotation, topic and reported speech particle って."""
from src.GrammarConstants import TTE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.toiu.index import match_toiu
from src.grammar_rules.rules.toiukotoha.index import match_toiukotoha
from src.grammar_rules.rules.toiunoha.index import match_toiunoha


def match_tte(tokens):
    longer_starts = {start for matcher in (match_toiu, match_toiukotoha, match_toiunoha)
                     for start, _ in matcher(tokens)}
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'って' or particle['lemma'] != 'って'
                or particle['pos'][:1] != ['助詞']
                or particle['span'][0] in longer_starts
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        # Sentence-initial だって is commonly a conjunction ("because/but").
        if i == 1 and previous['surface'] == 'だ':
            continue
        yield particle['span']


RULE = GrammarRule(id='tte', expression=TTE_EXPRESSION, reading='って',
                   meaning='quoting; as for; I hear that', match=match_tte)
