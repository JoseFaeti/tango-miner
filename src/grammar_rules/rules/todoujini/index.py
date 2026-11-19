"""Simultaneous events or properties expressed with と同時に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TODOUJINI_EXPRESSION = '〜と同時に'


def match_todoujini(tokens):
    for i in range(1, len(tokens) - 2):
        previous, to, douji, ni = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'],
                                        ['形容詞'], ['助動詞'])
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or douji['surface'] != '同時' or douji['lemma'] != '同時'
                or douji['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, to, douji), (to, douji, ni)))):
            continue
        yield to['span'][0], ni['span'][1]


RULE = GrammarRule(order=11, id='todoujini', expression=TODOUJINI_EXPRESSION,
                   reading='とどうじに', meaning='at the same time as; simultaneously',
                   match=match_todoujini)
RULES = (RULE,)
