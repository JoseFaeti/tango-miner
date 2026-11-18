"""Dependence expressed by a noun plus 次第 and a copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

SHIDAIDA_EXPRESSION = '〜次第だ'


def match_shidaida(tokens):
    for i in range(1, len(tokens) - 1):
        noun, marker, copula = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or marker['surface'] not in ('次第', 'しだい')
                or marker['lemma'] not in ('次第', 'しだい')
                or marker['pos'][:1] != ['名詞']
                or copula['pos'][:1] != ['助動詞']
                or not adjacent(noun, marker) or not adjacent(marker, copula)):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield marker['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=160, id='shidaida', expression=SHIDAIDA_EXPRESSION,
                   reading='しだいだ', meaning='depends on', match=match_shidaida)
RULES = (RULE,)
