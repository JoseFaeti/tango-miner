"""Unexpected succession expressed with かと思えば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜かと思えば'


def match_katoomoeba(tokens):
    for i in range(2, len(tokens) - 3):
        verb, past, ka, quote, think, ba = tokens[i - 2:i + 4]
        if (verb['pos'][:1] == ['動詞']
                and past['surface'] in ('た', 'だ') and past['pos'][:1] == ['助動詞']
                and ka['surface'] == 'か' and ka['pos'][:1] == ['助詞']
                and quote['surface'] == 'と' and quote['pos'][:2] == ['助詞', '格助詞']
                and think['surface'] in ('思え', 'おもえ')
                and think['lemma'] in ('思う', 'おもう') and think['pos'][:1] == ['動詞']
                and ba['surface'] == 'ば' and ba['pos'][:2] == ['助詞', '接続助詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 2, i + 3))):
            yield ka['span'][0], ba['span'][1]


RULE = GrammarRule(order=37, id='katoomoeba', expression=EXPRESSION,
                   reading='かとおもえば', meaning='just when; then unexpectedly',
                   match=match_katoomoeba)
RULES = (RULE,)
