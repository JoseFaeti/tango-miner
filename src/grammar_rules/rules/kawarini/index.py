"""Predicate + かわりに: substitution, compensation or contrast."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KAWARINI_EXPRESSION = '〜かわりに'


def match_kawarini(tokens):
    for i in range(1, len(tokens) - 1):
        previous, head, particle = tokens[i - 1:i + 2]
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        if not (predicate and adjacent(previous, head) and adjacent(head, particle)):
            continue
        if (head['surface'] in ('代わり', 'かわり')
                and head['lemma'] in ('代わり', 'かわり')
                and head['pos'][:1] == ['名詞']
                and particle['surface'] == particle['lemma'] == 'に'
                and particle['pos'][:1] == ['助詞']):
            yield head['span'][0], particle['span'][1]


RULE = GrammarRule(order=0, id='kawarini', expression=KAWARINI_EXPRESSION,
                   reading='かわりに', meaning='instead of; in exchange for; while',
                   match=match_kawarini)
RULES = (RULE,)
