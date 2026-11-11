"""Negative copula attached to a nominal predicate."""
from src.GrammarConstants import JANAI_DEWANAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_predicate_end


def match_janai_dewanai(tokens):
    for i in range(1, len(tokens) - 1):
        previous, copula = tokens[i - 1:i + 1]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['形状詞'])
                or not adjacent(previous, copula)):
            continue
        if (copula['surface'] == 'じゃ' and copula['lemma'] == 'だ'
                and copula['pos'][:1] == ['助動詞']):
            start = i + 1
        elif (copula['surface'] == 'で' and copula['pos'][:1] == ['助詞']
              and next_matches(tokens, i, 'は', '助詞', 'は')):
            start = i + 2
        else:
            continue
        if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
            continue
        end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
        if end is None:
            continue
        if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield copula['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='janai_dewanai', expression=JANAI_DEWANAI_EXPRESSION,
                   reading='じゃない / ではない', meaning='negative copula; is not',
                   match=match_janai_dewanai)
