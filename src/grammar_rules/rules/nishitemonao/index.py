"""Concessive にしても followed by the adverb なお."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.nishitemo.index import match_nishitemo


def match_nishitemonao(tokens):
    endings = {end: start for start, end in match_nishitemo(tokens)}
    for token in tokens:
        start, end = token['span']
        if (start in endings and token['surface'] in ('なお', '尚')
                and token['pos'][:1] == ['副詞']):
            yield endings[start], end


RULE = GrammarRule(order=16, id='nishitemonao', expression='〜にしてもなお',
                   reading='にしてもなお', meaning='even so; even then, still',
                   match=match_nishitemonao)

RULES = (RULE,)
