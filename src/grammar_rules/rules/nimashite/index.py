"""Noun + に増して: even more than."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMASHITE_EXPRESSION = '〜にまして'


def match_nimashite(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if (verb['surface'] == 'まして' and verb['lemma'] == 'まして'
                and verb['pos'][:1] == ['副詞']):
            yield ni['span'][0], verb['span'][1]
        elif (i + 2 < len(tokens) and verb['surface'] == '増し'
                and verb['lemma'] == '増す' and verb['pos'][:1] == ['動詞']
                and (te := tokens[i + 2])['surface'] == 'て'
                and te['pos'][:1] == ['助詞'] and adjacent(verb, te)):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='nimashite', expression=NIMASHITE_EXPRESSION,
                   reading='にまして', meaning='even more than', match=match_nimashite)
RULES = (RULE,)
