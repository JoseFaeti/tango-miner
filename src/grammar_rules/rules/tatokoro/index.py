"""Past verb followed by ところ, including resultative uses."""
from src.GrammarConstants import TATOKORO_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tatokoro(tokens):
    for i in range(2, len(tokens)):
        verb, past, marker = tokens[i - 2:i + 1]
        if (verb['pos'][:1] == ['動詞']
                and past['surface'] in ('た', 'だ')
                and past['lemma'] == past['surface']
                and past['pos'][:1] == ['助動詞']
                and marker['surface'] in ('ところ', '所')
                and marker['lemma'] in ('ところ', '所')
                and marker['pos'][:1] == ['名詞']
                and adjacent(verb, past) and adjacent(past, marker)):
            yield marker['span'][0], marker['span'][1]


RULE = GrammarRule(id='tatokoro', expression=TATOKORO_EXPRESSION,
                   reading='たところ', meaning='just did; upon doing, found that',
                   match=match_tatokoro)
