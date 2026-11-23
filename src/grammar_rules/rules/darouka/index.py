"""Questions and conjectures ending in だろうか."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

DAROUKA_EXPRESSION = '〜だろうか'


def match_darouka(tokens):
    for copula, ka in itertools.pairwise(tokens):
        if ((copula['lemma'], copula['surface']) in (('だ', 'だろう'), ('です', 'でしょう'))
                and copula['pos'][:1] == ['助動詞']
                and ka['surface'] == ka['lemma'] == 'か'
                and ka['pos'][:2] in (['助詞', '終助詞'], ['助詞', '副助詞'])
                and adjacent(copula, ka)):
            yield copula['span'][0], ka['span'][1]


RULE = GrammarRule(order=60, id='darouka', expression=DAROUKA_EXPRESSION,
                   reading='だろうか', meaning='I wonder whether; could it be',
                   match=match_darouka)
RULES = (RULE,)
