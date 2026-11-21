"""Conditional thoughts: 〜と思うと."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_to_omou_to(tokens):
    for i in range(1, len(tokens) - 2):
        previous, quote, think, ending = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith(('終止形', '連体形'))
                or quote['surface'] != 'と' or quote['pos'][:2] != ['助詞', '格助詞']
                or think['lemma'] not in ('思う', 'おもう')
                or think['surface'] not in ('思う', 'おもう') or think['pos'][:1] != ['動詞']
                or ending['surface'] != 'と' or ending['pos'][:2] != ['助詞', '接続助詞']
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 2))):
            continue
        yield quote['span'][0], ending['span'][1]


RULE = GrammarRule(order=148, id='to_omou_to', expression='〜と思うと',
                   reading='とおもうと', meaning='when thinking that; at the thought of',
                   match=match_to_omou_to)
RULES = (RULE,)
