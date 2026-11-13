"""Attached なんて expressing examples, surprise, or dismissal."""
NANTE_EXPRESSION = '〜なんて'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nante(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'なんて' or particle['lemma'] != 'なんて'
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=47, id='nante', expression=NANTE_EXPRESSION, reading='なんて',
                   meaning='such as; surprise or dismissal', match=match_nante)

RULES = (RULE,)
