"""Noun + にとどまらず: extending beyond the named example."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitodomarazu(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, zu = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('とどまら', '留まら', '止まら')
                and verb['lemma'] in ('とどまる', '留まる', '止まる')
                and verb['pos'][:1] == ['動詞']
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, zu)))):
            yield ni['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='nitodomarazu', expression='〜にとどまらず',
                   reading='にとどまらず', meaning='not limited to; extending beyond',
                   match=match_nitodomarazu)
RULES = (RULE,)
