"""Noun or pronoun + に基づいて: based on."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMOTOZUITE_EXPRESSION = '〜に基づいて'


def match_nimotozuite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('基づい', 'もとづい')
                or verb['lemma'] not in ('基づく', 'もとづく')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        tail = tokens[i + 2]
        if (tail['surface'] == 'て' and tail['pos'][:1] == ['助詞']
                and adjacent(verb, tail)):
            yield ni['span'][0], tail['span'][1]


RULE = GrammarRule(order=6, id='nimotozuite', expression=NIMOTOZUITE_EXPRESSION,
                   reading='にもとづいて', meaning='based on',
                   match=match_nimotozuite)

RULES = (RULE,)
