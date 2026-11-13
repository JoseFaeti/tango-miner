"""Noun + を目的に: for the purpose of an action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OMOKUTEKINI_EXPRESSION = '〜を目的に'


def match_omokutekini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, purpose, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and purpose['surface'] == '目的' and purpose['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, purpose), (wo, purpose, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='omokutekini', expression=OMOKUTEKINI_EXPRESSION,
                   reading='をもくてきに', meaning='for the purpose of',
                   match=match_omokutekini)
RULES = (RULE,)
