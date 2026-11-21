"""Unexpected succession or alternating situations with かと思えば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜かと思えば'


def match_katoomoeba(tokens):
    for i in range(1, len(tokens) - 3):
        previous = tokens[i - 1]
        ka, quote, think, ba = tokens[i:i + 4]
        past_verb = (i >= 2 and previous['surface'] in ('た', 'だ')
                     and previous['pos'][:1] == ['助動詞']
                     and tokens[i - 2]['pos'][:1] == ['動詞']
                     and adjacent(tokens[i - 2], previous))
        alternating = previous['pos'][:1] in (['動詞'], ['形容詞'], ['名詞'])
        if ((past_verb or alternating)
                and ka['surface'] == 'か' and ka['pos'][:1] == ['助詞']
                and quote['surface'] == 'と' and quote['pos'][:2] == ['助詞', '格助詞']
                and think['surface'] in ('思え', 'おもえ')
                and think['lemma'] in ('思う', 'おもう') and think['pos'][:1] == ['動詞']
                and ba['surface'] == 'ば' and ba['pos'][:2] == ['助詞', '接続助詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 3))):
            yield ka['span'][0], ba['span'][1]


RULE = GrammarRule(order=37, id='katoomoeba', expression=EXPRESSION,
                   reading='かとおもえば', meaning='just when; then unexpectedly; sometimes one situation, sometimes another',
                   match=match_katoomoeba)
RULES = (RULE,)
