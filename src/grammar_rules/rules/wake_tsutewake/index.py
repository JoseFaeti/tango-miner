"""Bare predicate-attached explanatory わけ."""
WAKE_TSUTEWAKE_EXPRESSION = '〜わけ / 〜ってわけ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_wake(tokens):
    for i in range(1, len(tokens)):
        previous, wake = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形'))
                or not adjacent(previous, wake)
                or wake['surface'] not in ('わけ', '訳')
                or wake['pos'][:2] != ['名詞', '普通名詞']):
            continue
        if i + 1 < len(tokens) and (not adjacent(wake, tokens[i + 1])
                                     or tokens[i + 1]['pos'][:1] != ['補助記号']):
            continue
        yield wake['span'][0], wake['span'][1]


RULE = GrammarRule(order=77, id='wake_tsutewake', expression=WAKE_TSUTEWAKE_EXPRESSION,
                   reading='わけ / ってわけ', meaning='that is why; that means',
                   match=match_wake)

RULES = (RULE,)
