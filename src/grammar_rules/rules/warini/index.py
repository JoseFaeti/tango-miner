"""Attached わりに: an outcome unexpected for a given standard."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

WARINI_EXPRESSION = '〜わりに'


def match_warini(tokens):
    for i in range(1, len(tokens) - 1):
        previous, head, ni = tokens[i - 1:i + 2]
        if (head['surface'] not in ('わり', '割', '割り')
                or head['lemma'] not in ('わり', '割', '割り')
                or head['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:1] != ['助詞']
                or not adjacent(previous, head) or not adjacent(head, ni)):
            continue
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        noun_no = (previous['surface'] == previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞']
                   and i >= 2 and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞'])
                   and adjacent(tokens[i - 2], previous))
        if predicate or noun_no:
            yield head['span'][0], ni['span'][1]


RULE = GrammarRule(id='warini', expression=WARINI_EXPRESSION, reading='わりに',
                   meaning='considering; for; although', match=match_warini)
RULES = (RULE,)
