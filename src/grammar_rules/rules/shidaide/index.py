"""Conditional dependence expressed by a noun plus 次第で."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SHIDAIDE_EXPRESSION = '〜次第で'


def match_shidaide(tokens):
    for i in range(1, len(tokens) - 1):
        noun, marker, particle = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or marker['surface'] not in ('次第', 'しだい')
                or marker['lemma'] not in ('次第', 'しだい')
                or marker['pos'][:1] != ['名詞']
                or particle['surface'] != 'で' or particle['lemma'] != 'で'
                or particle['pos'][:1] != ['助詞']
                or not adjacent(noun, marker) or not adjacent(marker, particle)):
            continue
        if (i + 2 < len(tokens) and adjacent(particle, tokens[i + 2])
                and tokens[i + 2]['surface'] == 'は'):
            continue
        yield marker['span'][0], particle['span'][1]


RULE = GrammarRule(order=160, id='shidaide', expression=SHIDAIDE_EXPRESSION,
                   reading='しだいで', meaning='depending on', match=match_shidaide)
RULES = (RULE,)
