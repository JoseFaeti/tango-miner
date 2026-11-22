"""Noun + に反し: contrary to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nihanshi(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] != '反し' or verb['lemma'] != '反する'
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if i + 2 < len(tokens):
            following = tokens[i + 2]
            if (adjacent(verb, following)
                    and following['pos'][:1] in (['助詞'], ['助動詞'], ['動詞'])):
                continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=10, id='nihanshi', expression='〜に反し',
                   reading='にはんし', meaning='contrary to; against',
                   match=match_nihanshi)

RULES = (RULE,)
