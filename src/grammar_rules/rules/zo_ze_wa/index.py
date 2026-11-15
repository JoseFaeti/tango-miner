"""Source-aligned emphatic final particles ぞ, ぜ and わ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ZO_ZE_WA_EXPRESSION = 'ぞ / ぜ / わ'


def match_zo_ze_wa(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('ぞ', 'ぜ', 'わ')
                or marker['lemma'] != marker['surface']
                or marker['pos'][:2] != ['助詞', '終助詞']
                or previous['pos'][:1] in (['空白'], ['補助記号'])
                or not adjacent(previous, marker)):
            continue
        yield marker['span']


RULE = GrammarRule(order=44, id='zo_ze_wa', expression=ZO_ZE_WA_EXPRESSION,
                   reading='ぞ / ぜ / わ', meaning='emphatic sentence ending',
                   match=match_zo_ze_wa)
RULES = (RULE,)
