"""Concessive にせよ / にしろ, including ambiguous literal imperatives."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NISEYO_NISHIRO_EXPRESSION = '〜にせよ / 〜にしろ'


def match_niseyo_nishiro(tokens):
    for i in range(1, len(tokens) - 1):
        previous, ni, verb = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('せよ', 'しろ')
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not adjacent(previous, ni) or not adjacent(ni, verb)):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=18, id='niseyo_nishiro', expression=NISEYO_NISHIRO_EXPRESSION,
                   reading='にせよ / にしろ', meaning='even if; whichever',
                   match=match_niseyo_nishiro)

RULES = (RULE,)
