"""Source-aligned subject and focus particle が."""
from src.GrammarConstants import GA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ga(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'が' or particle['lemma'] != 'が'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(id='ga', expression=GA_EXPRESSION, reading='が',
                   meaning='subject or focus marker', match=match_ga)
