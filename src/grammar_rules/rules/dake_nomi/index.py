"""Source-aligned limiting particles だけ and のみ."""
DAKE_NOMI_EXPRESSION = 'だけ / のみ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_dake_nomi(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] not in ('だけ', 'のみ')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=50, id='dake_nomi', expression=DAKE_NOMI_EXPRESSION,
                   reading='だけ / のみ', meaning='only; just', match=match_dake_nomi)

RULES = (RULE,)
