"""Attached temporal とき expressions."""
from src.GrammarConstants import TOKI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toki(tokens):
    for i in range(1, len(tokens)):
        previous, noun = tokens[i - 1:i + 1]
        if (noun['surface'] not in ('とき', '時') or noun['lemma'] not in ('とき', '時')
                or noun['pos'][:1] != ['名詞'] or not adjacent(previous, noun)):
            continue
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        nominal = (i >= 2 and previous['surface'] == 'の'
                   and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞']
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        if predicate or nominal:
            yield noun['span'][0], noun['span'][1]


RULE = GrammarRule(
    id='toki', expression=TOKI_EXPRESSION, reading='とき',
    meaning='when; at the time that', match=match_toki,
)
