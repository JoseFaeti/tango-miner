"""Noun + の極み: the height or extreme of something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NOKIWAMI_EXPRESSION = '〜の極み'


def match_nokiwami(tokens):
    for i in range(1, len(tokens) - 1):
        noun, no, extreme = tokens[i - 1:i + 2]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and no['surface'] == 'の' and no['pos'][:1] == ['助詞']
                and extreme['surface'] in ('極み', 'きわみ')
                and extreme['lemma'] in ('極み', 'きわみ')
                and extreme['pos'][:1] == ['名詞']
                and adjacent(noun, no) and adjacent(no, extreme)):
            yield no['span'][0], extreme['span'][1]


RULE = GrammarRule(order=6, id='nokiwami', expression=NOKIWAMI_EXPRESSION,
                   reading='のきわみ', meaning='the height of; the utmost', match=match_nokiwami)
RULES = (RULE,)
