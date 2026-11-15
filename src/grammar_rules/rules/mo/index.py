"""Source-aligned inclusive and focus particle も."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

MO_EXPRESSION = '〜も'


def match_mo(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'も' or particle['lemma'] != 'も'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=55, id='mo', expression=MO_EXPRESSION, reading='も',
                   meaning='also; too; even', match=match_mo)

RULES = (RULE,)
