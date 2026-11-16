"""Literary attributive copula after a role or identity noun."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TARU_EXPRESSION = '〜たる'


def match_taru(tokens):
    for i in range(1, len(tokens) - 1):
        before, copula, after = tokens[i - 1:i + 2]
        if (before['pos'][:1] == ['名詞'] and copula['surface'] == 'たる'
                and copula['lemma'] == 'たり' and copula['pos'][:1] == ['助動詞']
                and after['pos'][:1] == ['名詞']
                and adjacent(before, copula) and adjacent(copula, after)):
            yield copula['span'][0], copula['span'][1]


RULE = GrammarRule(id='taru', expression=TARU_EXPRESSION, reading='たる',
                   meaning='being; in the capacity of (literary)', match=match_taru)
RULES = (RULE,)
