"""Formal noun + に関し: concerning a topic."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikanshi(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] != '関し' or verb['lemma'] != '関する'
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        # Leave inflected verbs and longer particle constructions to their rules.
        if i + 2 < len(tokens):
            tail = tokens[i + 2]
            if adjacent(verb, tail) and tail['pos'][:1] in (['助詞'], ['助動詞']):
                continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=7, id='nikanshi', expression='〜に関し',
                   reading='にかんし', meaning='concerning; regarding (formal)',
                   match=match_nikanshi)
RULES = (RULE,)
