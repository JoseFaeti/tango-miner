"""Verb + ずに済む: avoid having to do something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tesumu.index import sumu_end
from src.grammar_rules.rules.zuni.index import match_zuni

ZUNISUMU_EXPRESSION = '〜ずにすむ'


def match_zunisumu(tokens):
    spans = {start: end for start, end in match_zuni(tokens)}
    for i in range(1, len(tokens) - 1):
        zu, ni, sumu = tokens[i - 1:i + 2]
        if (spans.get(zu['span'][0]) != ni['span'][1]
                or not adjacent(ni, sumu)
                or sumu['lemma'] not in ('済む', 'すむ')
                or sumu['pos'][:1] != ['動詞']):
            continue
        end = sumu_end(tokens, i + 1)
        if end is not None:
            yield zu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='zunisumu', expression=ZUNISUMU_EXPRESSION,
                   reading='ずにすむ', meaning='avoid having to do',
                   match=match_zunisumu, order=1)
RULES = (RULE,)
