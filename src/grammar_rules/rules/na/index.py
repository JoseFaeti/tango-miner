"""Reflective or prohibitive sentence-final な."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NA_EXPRESSION = '〜な'


def match_na(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('な', 'なあ')
                or marker['lemma'] != marker['surface']
                or marker['pos'][:2] != ['助詞', '終助詞']
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'],
                                               ['助動詞'], ['名詞'], ['副詞'], ['代名詞'])
                or not adjacent(previous, marker)):
            continue
        yield marker['span'][0], marker['span'][1]


RULE = GrammarRule(order=63, id='na', expression=NA_EXPRESSION, reading='な',
                   meaning='reflective sentence ending; do not (prohibition)', match=match_na)
RULES = (RULE,)
