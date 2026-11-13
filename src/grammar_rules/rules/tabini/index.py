"""Attached recurring-event たびに expressions."""
TABINI_EXPRESSION = '〜たびに'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tabini(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, particle = tokens[i - 1:i + 2]
        verb = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                and previous['pos'][5].startswith('連体形'))
        nominal = (i >= 2 and previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞']
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        if not (verb or nominal):
            continue
        if (noun['surface'] not in ('たび', '度') or noun['lemma'] not in ('たび', '度')
                or noun['pos'][:1] != ['名詞']
                or particle['surface'] != 'に' or particle['lemma'] != 'に'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, noun) or not adjacent(noun, particle)):
            continue
        yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=104,
    id='tabini', expression=TABINI_EXPRESSION, reading='たびに',
    meaning='whenever; every time', match=match_tabini,
)

RULES = (RULE,)
