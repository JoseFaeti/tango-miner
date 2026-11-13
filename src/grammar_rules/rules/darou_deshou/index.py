"""Conjectural and confirmation auxiliary endings."""
DAROU_DESHOU_EXPRESSION = 'だろう / でしょう'
from src.grammar_rules.base import GrammarRule


def match_darou_deshou(tokens):
    for token in tokens:
        if (token['pos'][:1] == ['助動詞']
                and (token['lemma'], token['surface']) in
                (('だ', 'だろう'), ('です', 'でしょう'), ('です', 'でしょ'))):
            yield tuple(token['span'])


RULE = GrammarRule(order=164, id='darou_deshou', expression=DAROU_DESHOU_EXPRESSION,
                   reading='だろう / でしょう', meaning='probably; I suppose; right?',
                   match=match_darou_deshou)

RULES = (RULE,)
