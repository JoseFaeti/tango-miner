"""Noun or pronoun + に則り: in accordance with a standard."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜に則り'


def match_ninotsutori(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('則り', 'のっとり')
                or verb['lemma'] not in ('則る', 'のっとる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if (i + 2 < len(tokens) and adjacent(verb, tokens[i + 2])
                and tokens[i + 2]['pos'][:1] == ['助動詞']):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=6, id='ninotsutori', expression=EXPRESSION,
                   reading='にのっとり', meaning='in accordance with',
                   match=match_ninotsutori)
RULES = (RULE,)
