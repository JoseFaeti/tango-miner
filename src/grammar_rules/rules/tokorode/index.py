"""Past verb followed by concessive ところで."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOKORODE_EXPRESSION = '〜ところで'


def match_tokorode(tokens):
    for i in range(2, len(tokens) - 1):
        verb, past, noun, particle = tokens[i - 2:i + 2]
        if (verb['pos'][:1] == ['動詞']
                and past['surface'] in ('た', 'だ')
                and past['lemma'] == past['surface']
                and past['pos'][:1] == ['助動詞']
                and noun['surface'] in ('ところ', '所')
                and noun['lemma'] in ('ところ', '所')
                and noun['pos'][:1] == ['名詞']
                and particle['surface'] == particle['lemma'] == 'で'
                and particle['pos'][:1] == ['助詞']
                and adjacent(verb, past) and adjacent(past, noun)
                and adjacent(noun, particle)):
            yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=105, id='tokorode', expression=TOKORODE_EXPRESSION,
                   reading='ところで', meaning='even if; even though',
                   match=match_tokorode)
RULES = (RULE,)
