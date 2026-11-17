"""A time or event marks the end of an activity."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKAGIRINI_EXPRESSION = '〜を限りに'


def match_okagirini(tokens):
    for i in range(1, len(tokens) - 2):
        anchor, wo, kagiri, ni = tokens[i - 1:i + 3]
        if (anchor['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and kagiri['surface'] in ('限り', 'かぎり')
                and kagiri['lemma'] in ('限り', '限る', 'かぎり', 'かぎる')
                and kagiri['pos'][:1] in (['名詞'], ['動詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and adjacent(anchor, wo) and adjacent(wo, kagiri) and adjacent(kagiri, ni)):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(order=3, id='okagirini', expression=OKAGIRINI_EXPRESSION,
                   reading='をかぎりに', meaning='ending with; as of (a time or event)',
                   match=match_okagirini)
RULES = (RULE,)
