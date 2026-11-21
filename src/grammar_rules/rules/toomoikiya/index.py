"""Contrary expectation expressed by と思いきや."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOOMOIKIYA_EXPRESSION = '〜と思いきや'


def match_toomoikiya(tokens):
    for i in range(len(tokens) - 3):
        quote, think, ki, ya = tokens[i:i + 4]
        if (quote['surface'] == 'と' and quote['pos'][:2] == ['助詞', '格助詞']
                and think['surface'] in ('思い', 'おもい')
                and think['lemma'] in ('思う', 'おもう') and think['pos'][:1] == ['動詞']
                and ki['surface'] == 'き' and ki['pos'][:1] == ['助動詞']
                and ya['surface'] == 'や' and ya['pos'][:1] == ['助詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i, i + 3))):
            yield quote['span'][0], ya['span'][1]


RULE = GrammarRule(id='toomoikiya', expression=TOOMOIKIYA_EXPRESSION,
                   reading='とおもいきや', meaning='contrary to expectation',
                   match=match_toomoikiya)
RULES = (RULE,)
