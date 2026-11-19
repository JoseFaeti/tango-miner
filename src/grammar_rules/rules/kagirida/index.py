"""Extreme degree expressed with an adjective and 限りだ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KAGIRIDA_EXPRESSION = '〜限りだ'


def match_kagirida(tokens):
    for i in range(1, len(tokens) - 1):
        previous, kagiri, copula = tokens[i - 1:i + 2]
        pos = previous['pos']
        adjective = (pos[:1] == ['形容詞'] and len(pos) >= 6
                     and pos[5].startswith('連体形'))
        na_adjective = (previous['surface'] == 'な' and previous['lemma'] == 'だ'
                        and pos[:1] == ['助動詞'] and i >= 2
                        and tokens[i - 2]['pos'][:1] == ['形状詞']
                        and adjacent(tokens[i - 2], previous))
        if not (adjective or na_adjective):
            continue
        if (kagiri['surface'] not in ('限り', 'かぎり')
                or kagiri['lemma'] not in ('限り', '限る', 'かぎり', 'かぎる')
                or kagiri['pos'][:1] not in (['名詞'], ['動詞'])
                or (copula['lemma'], copula['surface']) not in (('だ', 'だ'), ('です', 'です'))
                or copula['pos'][:1] != ['助動詞']
                or not adjacent(previous, kagiri) or not adjacent(kagiri, copula)):
            continue
        yield kagiri['span'][0], copula['span'][1]


RULE = GrammarRule(order=3, id='kagirida', expression=KAGIRIDA_EXPRESSION,
                   reading='かぎりだ', meaning='extremely; as can be', match=match_kagirida)
RULES = (RULE,)
