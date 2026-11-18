"""Repeated immediate succession with a verb plus そばから."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SOBAKARA_EXPRESSION = '〜そばから'


def match_sobakara(tokens):
    for i in range(1, len(tokens) - 1):
        previous, soba, kara = tokens[i - 1:i + 2]
        if not (soba['surface'] in ('そば', '側')
                and soba['lemma'] in ('そば', '側')
                and soba['pos'][:1] == ['名詞']
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:1] == ['助詞']
                and adjacent(previous, soba) and adjacent(soba, kara)):
            continue
        finite = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                  and previous['pos'][5].startswith('連体形'))
        past = (previous['surface'] in ('た', 'だ') and previous['lemma'] in ('た', 'だ')
                and previous['pos'][:1] == ['助動詞'] and i >= 2
                and tokens[i - 2]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 2], previous))
        if finite or past:
            yield soba['span'][0], kara['span'][1]


RULE = GrammarRule(order=160, id='sobakara', expression=SOBAKARA_EXPRESSION,
                   reading='そばから', meaning='repeatedly, as soon as',
                   match=match_sobakara)
RULES = (RULE,)
