"""Noun + に替えて: exchange for or replace with the noun."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikaete_exchange(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] == '替え' and verb['lemma'] == '替える'
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=0, id='nikaete_exchange', expression='〜に替えて',
                   reading='にかえて', meaning='in exchange for; replacing with',
                   match=match_nikaete_exchange)
RULES = (RULE,)
