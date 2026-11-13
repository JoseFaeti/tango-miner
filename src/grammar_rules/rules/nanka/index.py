"""Attached なんか expressing examples, emphasis, or dismissal."""
NANKA_EXPRESSION = '〜なんか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nanka(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'なんか' or particle['lemma'] != 'なんか'
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=48, id='nanka', expression=NANKA_EXPRESSION, reading='なんか',
                   meaning='such as; emphasis or dismissal', match=match_nanka)

RULES = (RULE,)
