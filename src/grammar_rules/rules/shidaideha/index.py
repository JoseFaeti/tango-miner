"""Conditional dependence expressed by a noun plus 次第では."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SHIDAIDEHA_EXPRESSION = '〜次第では'


def match_shidaideha(tokens):
    for i in range(1, len(tokens) - 2):
        noun, marker, de, wa = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or marker['surface'] not in ('次第', 'しだい')
                or marker['lemma'] not in ('次第', 'しだい')
                or marker['pos'][:1] != ['名詞']
                or de['surface'] != 'で' or de['lemma'] != 'で'
                or de['pos'][:1] != ['助詞']
                or wa['surface'] != 'は' or wa['lemma'] != 'は'
                or wa['pos'][:1] != ['助詞']
                or not adjacent(noun, marker) or not adjacent(marker, de)
                or not adjacent(de, wa)):
            continue
        yield marker['span'][0], wa['span'][1]


RULE = GrammarRule(order=160, id='shidaideha', expression=SHIDAIDEHA_EXPRESSION,
                   reading='しだいでは', meaning='depending on circumstances',
                   match=match_shidaideha)
RULES = (RULE,)
