"""Formal concessive といえども."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOIEDOMO_EXPRESSION = '〜といえども'


def match_toiedomo(tokens):
    for i in range(len(tokens) - 3):
        particle, verb, do, mo = tokens[i:i + 4]
        if (particle['surface'] == 'と' and particle['pos'][:2] == ['助詞', '格助詞']
                and verb['surface'] in ('いえ', '言え')
                and verb['lemma'] in ('いう', '言う') and verb['pos'][:1] == ['動詞']
                and do['surface'] == 'ど' and do['pos'][:2] == ['助詞', '接続助詞']
                and mo['surface'] == 'も' and mo['pos'][:2] == ['助詞', '係助詞']
                and all(adjacent(a, b) for a, b in zip((particle, verb, do), (verb, do, mo)))):
            yield particle['span'][0], mo['span'][1]


RULE = GrammarRule(order=2, id='toiedomo', expression=TOIEDOMO_EXPRESSION,
                   reading='といえども', meaning='even; although', match=match_toiedomo)
RULES = (RULE,)
