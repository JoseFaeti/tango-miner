"""Attached temporal うちに expressions."""
from src.GrammarConstants import UCHINI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_uchini(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, particle = tokens[i - 1:i + 2]
        if noun['surface'] not in ('うち', '内') or particle['surface'] != 'に':
            continue
        nominal = (i >= 2 and previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞']
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        if not (nominal or predicate):
            continue
        if (noun['surface'] not in ('うち', '内') or noun['pos'][:1] != ['名詞']
                or particle['surface'] != 'に' or particle['lemma'] != 'に'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, noun) or not adjacent(noun, particle)):
            continue
        yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(
    id='uchini', expression=UCHINI_EXPRESSION, reading='うちに',
    meaning='while; before a change occurs', match=match_uchini,
)
