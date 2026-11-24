"""Formal connective noun or dictionary verb + に従い."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitagai(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, following = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] in (['名詞'], ['代名詞'], ['動詞'])
                and (preceding['pos'][:1] != ['動詞']
                     or preceding['surface'] == preceding['lemma'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('従い', 'したがい')
                and verb['lemma'] in ('従う', 'したがう') and verb['pos'][:1] == ['動詞']
                and adjacent(preceding, ni) and adjacent(ni, verb)
                and following['pos'][:1] not in (['助動詞'], ['助詞'])
                and following['surface'] not in ('。', '！', '？', '!', '?')):
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=11, id='nishitagai', expression='〜にしたがい',
                   reading='にしたがい', meaning='as; in accordance with',
                   match=match_nishitagai)
RULES = (RULE,)
