"""Noun + にも増して: even more than."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMOMASHITE_EXPRESSION = '〜にもまして'


def match_nimomashite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, mo, verb = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or not adjacent(noun, ni) or not adjacent(ni, mo) or not adjacent(mo, verb)):
            continue
        if (verb['surface'] == 'まして' and verb['lemma'] == 'まして'
                and verb['pos'][:1] == ['副詞']):
            yield ni['span'][0], verb['span'][1]
        elif (i + 3 < len(tokens) and verb['surface'] == '増し'
                and verb['lemma'] == '増す' and verb['pos'][:1] == ['動詞']
                and (te := tokens[i + 3])['surface'] == 'て'
                and te['pos'][:1] == ['助詞'] and adjacent(verb, te)):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='nimomashite', expression=NIMOMASHITE_EXPRESSION,
                   reading='にもまして', meaning='even more than', match=match_nimomashite)
RULES = (RULE,)
