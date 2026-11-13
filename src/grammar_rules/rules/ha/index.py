"""Source-aligned topic and contrast particle は."""
HA_EXPRESSION = '〜は'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ha(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'は' or particle['lemma'] != 'は'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=54, id='ha', expression=HA_EXPRESSION, reading='は',
                   meaning='as for; topic or contrast marker', match=match_ha)

RULES = (RULE,)
