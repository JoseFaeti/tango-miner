"""Noun + をきっかけに: an event prompting a change."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKITSUKAKENI_EXPRESSION = '〜をきっかけに'


def match_okitsukakeni(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, trigger, ni = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and trigger['surface'] in ('きっかけ', '切っ掛け')
                and trigger['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, trigger), (wo, trigger, ni)))):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(id='okitsukakeni', expression=OKITSUKAKENI_EXPRESSION,
                   reading='をきっかけに', meaning='triggered by; taking as an opportunity',
                   match=match_okitsukakeni)
RULES = (RULE,)
