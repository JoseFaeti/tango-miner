"""Noun + を契機に: an event prompting a change."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKEIKINI_EXPRESSION = '〜を契機に'


def match_okeikini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, trigger, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and trigger['surface'] == '契機'
                and trigger['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, trigger), (wo, trigger, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='okeikini', expression=OKEIKINI_EXPRESSION,
                   reading='をけいきに', meaning='triggered by; taking as an opportunity',
                   match=match_okeikini)
RULES = (RULE,)
