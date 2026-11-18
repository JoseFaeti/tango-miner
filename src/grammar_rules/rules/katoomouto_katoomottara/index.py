"""Rapid succession expressed with かと思うと / かと思ったら."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KATOOMOUTO_KATOOMOTTARA_EXPRESSION = '〜かと思うと / 〜かと思ったら'


def match_katoomouto_katoomottara(tokens):
    for i in range(2, len(tokens) - 3):
        verb, past, ka, quote, think, ending = tokens[i - 2:i + 4]
        if (verb['pos'][:1] != ['動詞']
                or past['surface'] not in ('た', 'だ')
                or past['pos'][:1] != ['助動詞']
                or ka['surface'] != 'か' or ka['pos'][:1] != ['助詞']
                or quote['surface'] != 'と' or quote['pos'][:2] != ['助詞', '格助詞']
                or think['lemma'] not in ('思う', 'おもう') or think['pos'][:1] != ['動詞']
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 2, i + 3))):
            continue
        present = (think['surface'] in ('思う', 'おもう') and ending['surface'] == 'と'
                   and ending['pos'][:2] == ['助詞', '接続助詞'])
        conditional = (think['surface'] in ('思っ', 'おもっ') and ending['surface'] == 'たら'
                       and ending['lemma'] == 'た' and ending['pos'][:1] == ['助動詞'])
        if present or conditional:
            yield ka['span'][0], ending['span'][1]


RULE = GrammarRule(order=37, id='katoomouto_katoomottara',
                   expression=KATOOMOUTO_KATOOMOTTARA_EXPRESSION,
                   reading='かとおもうと / かとおもったら',
                   meaning='as soon as; no sooner than', match=match_katoomouto_katoomottara)
RULES = (RULE,)
