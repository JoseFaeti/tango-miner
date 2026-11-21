"""Conditional thought or unexpected discovery with と思ったら."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜と思ったら'


def match_toomottara(tokens):
    for i in range(1, len(tokens) - 2):
        quote, think, ending = tokens[i:i + 3]
        if (quote['surface'] != 'と' or quote['pos'][:2] != ['助詞', '格助詞']
                or think['surface'] not in ('思っ', 'おもっ')
                or think['lemma'] not in ('思う', 'おもう') or think['pos'][:1] != ['動詞']
                or ending['surface'] != 'たら' or ending['lemma'] != 'た'
                or ending['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 2], tokens[i:i + 3]))):
            continue
        # The established rapid-succession rule owns past verb + かと思ったら.
        if (i >= 3 and tokens[i - 1]['surface'] == 'か'
                and tokens[i - 1]['pos'][:1] == ['助詞']
                and tokens[i - 2]['surface'] in ('た', 'だ')
                and tokens[i - 2]['pos'][:1] == ['助動詞']
                and tokens[i - 3]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 3], tokens[i - 2])
                and adjacent(tokens[i - 2], tokens[i - 1])):
            continue
        yield quote['span'][0], ending['span'][1]


RULE = GrammarRule(id='toomottara', expression=EXPRESSION, reading='とおもったら',
                   meaning='when I thought; but then; upon thinking', match=match_toomottara)
RULES = (RULE,)
