"""Noun + に言わせると, expressing someone's perspective."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


EXPRESSION = '〜に言わせると'


def match_niiwaseruto(tokens):
    for i in range(1, len(tokens) - 3):
        noun, ni, verb, causative, to = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('言わ', 'いわ')
                or verb['lemma'] not in ('言う', 'いう') or verb['pos'][:1] != ['動詞']
                or causative['surface'] != 'せる' or causative['lemma'] != 'せる'
                or causative['pos'][:1] != ['助動詞']
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, ni, verb, causative), (ni, verb, causative, to)))):
            continue
        yield ni['span'][0], to['span'][1]


RULE = GrammarRule(order=8, id='niiwaseruto', expression=EXPRESSION,
                   reading='にいわせると', meaning="in someone's opinion",
                   match=match_niiwaseruto)
RULES = (RULE,)
