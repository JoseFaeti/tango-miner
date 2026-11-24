"""Formal connective noun + に応じ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_niouji(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, following = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('応じ', 'おうじ')
                and verb['lemma'] in ('応じる', '応ずる', 'おうじる', 'おうずる') and verb['pos'][:1] == ['動詞']
                and adjacent(preceding, ni) and adjacent(ni, verb)
                and following['pos'][:1] not in (['助動詞'], ['助詞'])
                and following['surface'] not in ('。', '！', '？', '!', '?')):
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=7, id='niouji', expression='〜に応じ',
                   reading='におうじ', meaning='depending on; in accordance with', match=match_niouji)
RULES = (RULE,)
