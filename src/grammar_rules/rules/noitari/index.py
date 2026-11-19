"""Noun + の至り: an extreme state or a consequence of a quality."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NOITARI_EXPRESSION = '〜の至り'


def match_noitari(tokens):
    for i in range(1, len(tokens) - 1):
        noun, no, itari = tokens[i - 1:i + 2]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and no['surface'] == 'の' and no['pos'][:1] == ['助詞']
                and itari['surface'] in ('至り', 'いたり')
                and itari['lemma'] in ('至り', 'いたり')
                and itari['pos'][:1] == ['名詞']
                and adjacent(noun, no) and adjacent(no, itari)):
            yield no['span'][0], itari['span'][1]


RULE = GrammarRule(order=6, id='noitari', expression=NOITARI_EXPRESSION,
                   reading='のいたり', meaning='the utmost; a result of', match=match_noitari)
RULES = (RULE,)
