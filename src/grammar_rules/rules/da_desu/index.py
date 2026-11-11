"""Present-tense finite copula and polite predicate ending."""
from src.GrammarConstants import DA_DESU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_da_desu(tokens):
    for previous, token in zip(tokens, tokens[1:]):
        if (token['pos'][:1] == ['助動詞']
                and (token['lemma'], token['surface']) in (('だ', 'だ'), ('です', 'です'))
                and adjacent(previous, token)
                and previous['pos'][:1] not in (['空白'], ['補助記号'])):
            yield token['span'][0], token['span'][1]


RULE = GrammarRule(id='da_desu', expression=DA_DESU_EXPRESSION,
                   reading='だ / です', meaning='copula; polite predicate ending',
                   match=match_da_desu)
