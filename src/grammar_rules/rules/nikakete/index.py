"""Noun + にかけて: extending over a period or area."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikakete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('かけ', '掛け')
                and verb['lemma'] in ('かける', '掛ける')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=-1, id='nikakete', expression='〜にかけて',
                   reading='にかけて', meaning='through a period or over an area',
                   match=match_nikakete)
RULES = (RULE,)
