"""Noun-linking and nominalizing particle の."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_no(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'の' or particle['lemma'] != 'の'
                or particle['pos'][:2] not in (['助詞', '格助詞'], ['助詞', '準体助詞'])
                or previous['pos'][:1] == ['空白'] or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=53, id='no', expression='〜の', reading='の',
                   meaning='possession or noun relation; nominalization', match=match_no)
RULES = (RULE,)
