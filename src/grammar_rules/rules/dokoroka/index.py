"""Source-aligned どころか contrast construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

DOKOROKA_EXPRESSION = '〜どころか'


def match_dokoroka(tokens):
    for i in range(1, len(tokens) - 1):
        previous, dokoro, ka = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or dokoro['surface'] != 'どころ' or dokoro['lemma'] != 'どころ'
                or dokoro['pos'][:2] != ['助詞', '副助詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, dokoro) or not adjacent(dokoro, ka)):
            continue
        yield dokoro['span'][0], ka['span'][1]


RULE = GrammarRule(order=2, id='dokoroka', expression=DOKOROKA_EXPRESSION,
                   reading='どころか', meaning='far from; let alone; on the contrary',
                   match=match_dokoroka)
RULES = (RULE,)
