"""Noun + まみれ, covered with a substance."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

MAMIRE_EXPRESSION = '〜まみれ'


def match_mamire(tokens):
    for i in range(1, len(tokens)):
        noun, suffix = tokens[i - 1:i + 1]
        if (noun['pos'][:1] == ['名詞'] and adjacent(noun, suffix)
                and suffix['surface'] == suffix['lemma'] == 'まみれ'
                and suffix['pos'][:2] == ['接尾辞', '名詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='mamire', expression=MAMIRE_EXPRESSION,
                   reading='まみれ', meaning='covered with; smeared with',
                   match=match_mamire)
RULES = (RULE,)
