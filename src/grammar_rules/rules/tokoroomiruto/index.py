"""Attributive predicate + ところを見ると, an inference from evidence."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tokoroo.index import match_tokoroo


EXPRESSION = '〜ところを見ると'


def match_tokoroomiruto(tokens):
    prefixes = set(match_tokoroo(tokens))
    for i in range(len(tokens) - 3):
        noun, particle, verb, ending = tokens[i:i + 4]
        if ((noun['span'][0], particle['span'][1]) in prefixes
                and verb['surface'] in ('見る', 'みる')
                and verb['lemma'] in ('見る', 'みる')
                and verb['pos'][:1] == ['動詞']
                and ending['surface'] == ending['lemma'] == 'と'
                and ending['pos'][:1] == ['助詞']
                and adjacent(particle, verb) and adjacent(verb, ending)):
            yield noun['span'][0], ending['span'][1]


RULE = GrammarRule(order=103, id='tokoroomiruto', expression=EXPRESSION,
                   reading='ところをみると', meaning='judging from; considering',
                   match=match_tokoroomiruto)
RULES = (RULE,)
