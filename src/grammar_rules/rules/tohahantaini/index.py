"""Contrast with a preceding noun or proposition using とは反対に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOHAHANTAINI_EXPRESSION = '〜とは反対に'


def match_tohahantaini(tokens):
    for i in range(1, len(tokens) - 3):
        target = tokens[i - 1]
        phrase = tokens[i:i + 4]
        if (target['pos'][:1] in (['空白'], ['補助記号'])
                or [(t['surface'], t['pos'][0]) for t in phrase[:3]]
                != [('と', '助詞'), ('は', '助詞'), ('反対', '名詞')]
                or phrase[3]['surface'] != 'に'
                or phrase[3]['pos'][:1] not in (['助詞'], ['助動詞'])
                or not all(adjacent(a, b) for a, b in zip([target] + phrase, phrase))):
            continue
        yield phrase[0]['span'][0], phrase[-1]['span'][1]


RULE = GrammarRule(order=6, id='tohahantaini', expression=TOHAHANTAINI_EXPRESSION,
                   reading='とははんたいに', meaning='contrary to; opposite to',
                   match=match_tohahantaini)
RULES = (RULE,)
