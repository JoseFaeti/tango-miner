"""Question and uncertainty particle か."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KA_EXPRESSION = 'か'


def match_ka(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] != 'か' or marker['lemma'] != 'か'
                or marker['pos'][:2] != ['助詞', '終助詞']
                or previous['pos'][:1] in (['空白'], ['補助記号'])
                or not adjacent(previous, marker)):
            continue
        yield tuple(marker['span'])


RULE = GrammarRule(order=53, id='ka', expression=KA_EXPRESSION, reading='か',
                   meaning='question; uncertainty', match=match_ka)

RULES = (RULE,)
