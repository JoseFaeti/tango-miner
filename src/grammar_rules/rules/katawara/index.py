"""Concurrent activities expressed by verb + かたわら."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


EXPRESSION = '〜かたわら'


def match_katawara(tokens):
    for i in range(1, len(tokens)):
        previous, word = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (word['surface'] not in ('かたわら', '傍ら')
                or word['lemma'] not in ('傍ら', 'かたわら')
                or word['pos'][:1] != ['名詞']
                or pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith('連体形')
                or not adjacent(previous, word)):
            continue
        yield word['span'][0], word['span'][1]


RULE = GrammarRule(id='katawara', expression=EXPRESSION, reading='かたわら',
                   meaning='while also; alongside another activity', match=match_katawara)
RULES = (RULE,)
