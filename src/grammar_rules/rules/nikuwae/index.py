"""Noun + に加え: in addition to."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKUWAE_EXPRESSION = '〜に加え'


def match_nikuwae(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('加え', 'くわえ')
                or verb['lemma'] not in ('加える', 'くわえる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if (i + 2 < len(tokens) and adjacent(verb, tokens[i + 2])
                and tokens[i + 2]['pos'][:1] in (['助動詞'], ['助詞'])):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=-1, id='nikuwae', expression=NIKUWAE_EXPRESSION,
                   reading='にくわえ', meaning='in addition to', match=match_nikuwae)
RULES = (RULE,)
