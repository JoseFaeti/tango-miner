"""Noun + にかまけて: being preoccupied with something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAMAKETE_EXPRESSION = '〜にかまけて'


def match_nikamakete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('かまけ', '感け')
                and verb['lemma'] in ('かまける', '感ける')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and adjacent(noun, ni) and adjacent(ni, verb) and adjacent(verb, te)):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='nikamakete', expression=NIKAMAKETE_EXPRESSION,
                   reading='にかまけて', meaning='preoccupied with something',
                   match=match_nikamakete)
RULES = (RULE,)
