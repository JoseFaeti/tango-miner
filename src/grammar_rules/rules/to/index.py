"""Source-aligned quotation, accompaniment and listing particle と."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_to(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'と' or particle['lemma'] != 'と'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=53, id='to', expression='〜と', reading='と',
                   meaning='quotation; with; and', match=match_to)

RULES = (RULE,)
