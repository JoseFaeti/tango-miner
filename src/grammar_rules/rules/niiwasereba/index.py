"""Noun + に言わせれば, expressing someone's perspective."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


EXPRESSION = '〜に言わせれば'


def match_niiwasereba(tokens):
    for i in range(1, len(tokens) - 3):
        noun, ni, verb, causative, ba = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('言わ', 'いわ')
                or verb['lemma'] not in ('言う', 'いう') or verb['pos'][:1] != ['動詞']
                or causative['surface'] != 'せれ' or causative['lemma'] != 'せる'
                or causative['pos'][:1] != ['助動詞']
                or ba['surface'] != 'ば' or ba['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, ni, verb, causative), (ni, verb, causative, ba)))):
            continue
        yield ni['span'][0], ba['span'][1]


RULE = GrammarRule(order=8, id='niiwasereba', expression=EXPRESSION,
                   reading='にいわせれば', meaning="in someone's opinion",
                   match=match_niiwasereba)
RULES = (RULE,)
