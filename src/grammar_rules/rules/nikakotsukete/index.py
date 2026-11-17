"""Noun + にかこつけて: using something as a pretext."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAKOTSUKETE_EXPRESSION = '〜にかこつけて'


def match_nikakotsukete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('かこつけ', '託け')
                and verb['lemma'] in ('かこつける', '託ける')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and adjacent(noun, ni) and adjacent(ni, verb) and adjacent(verb, te)):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=6, id='nikakotsukete', expression=NIKAKOTSUKETE_EXPRESSION,
                   reading='にかこつけて', meaning='using something as a pretext',
                   match=match_nikakotsukete)
RULES = (RULE,)
