"""A time or event marks the last occurrence of an activity."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OSAIGONI_EXPRESSION = '〜を最後に'


def match_osaigoni(tokens):
    for i in range(1, len(tokens) - 2):
        anchor, wo, saigo, ni = tokens[i - 1:i + 3]
        if (anchor['pos'][:1] in (['名詞'], ['代名詞'])
                and wo['surface'] == 'を' and wo['pos'][:2] == ['助詞', '格助詞']
                and saigo['surface'] in ('最後', 'さいご')
                and saigo['lemma'] in ('最後', 'さいご')
                and saigo['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:2] == ['助詞', '格助詞']
                and adjacent(anchor, wo) and adjacent(wo, saigo) and adjacent(saigo, ni)):
            yield wo['span'][0], ni['span'][1]


RULE = GrammarRule(order=3, id='osaigoni', expression=OSAIGONI_EXPRESSION,
                   reading='をさいごに', meaning='ending with; after which no more',
                   match=match_osaigoni)
RULES = (RULE,)
