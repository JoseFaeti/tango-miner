"""Noun or pronoun + に沿って: along or in accordance with."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NISOTSUTE_EXPRESSION = '〜に沿って'


def match_nisotsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('沿っ', 'そっ')
                or verb['lemma'] not in ('沿う', 'そう')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        tail = tokens[i + 2]
        if (tail['surface'] == 'て' and tail['pos'][:1] == ['助詞']
                and adjacent(verb, tail)):
            yield ni['span'][0], tail['span'][1]


RULE = GrammarRule(order=6, id='nisotsute', expression=NISOTSUTE_EXPRESSION,
                   reading='にそって', meaning='along; in accordance with',
                   match=match_nisotsute)

RULES = (RULE,)
