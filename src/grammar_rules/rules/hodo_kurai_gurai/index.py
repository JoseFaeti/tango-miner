"""Source-aligned degree and approximate-quantity particles."""
HODO_KURAI_GURAI_EXPRESSION = 'ほど / くらい / ぐらい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_hodo_kurai_gurai(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] not in ('ほど', '程', 'くらい', 'ぐらい')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=51, id='hodo_kurai_gurai', expression=HODO_KURAI_GURAI_EXPRESSION,
                   reading='ほど / くらい / ぐらい',
                   meaning='degree; extent; approximately', match=match_hodo_kurai_gurai)

RULES = (RULE,)
