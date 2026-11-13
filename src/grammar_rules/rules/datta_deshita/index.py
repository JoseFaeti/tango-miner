"""Past copula and polite predicate ending."""
DATTA_DESHITA_EXPRESSION = 'だった / でした'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_datta_deshita(tokens):
    for i in range(1, len(tokens) - 1):
        previous, copula = tokens[i - 1:i + 1]
        if (copula['pos'][:1] == ['助動詞']
                and (copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
                and adjacent(previous, copula)
                and previous['pos'][:1] not in (['空白'], ['補助記号'])
                and next_matches(tokens, i, 'た', '助動詞', 'た')):
            yield copula['span'][0], tokens[i + 1]['span'][1]


RULE = GrammarRule(order=166, id='datta_deshita', expression=DATTA_DESHITA_EXPRESSION,
                   reading='だった / でした', meaning='was; were (でした is polite)',
                   match=match_datta_deshita)

RULES = (RULE,)
