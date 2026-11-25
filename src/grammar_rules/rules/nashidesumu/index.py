"""Noun + なしで済む: manage without something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tesumu.index import sumu_end

NASHIDESUMU_EXPRESSION = '〜なしですむ'


def match_nashidesumu(tokens):
    for i in range(1, len(tokens) - 2):
        antecedent, nashi, connective, sumu = tokens[i - 1:i + 3]
        if (antecedent['pos'][:1] not in (['名詞'], ['代名詞'])
                or not adjacent(antecedent, nashi)
                or nashi['surface'] not in ('なし', '無し')
                or nashi['pos'][:1] != ['名詞']
                or connective['surface'] != 'で'
                or connective['pos'][:1] != ['助詞']
                or not adjacent(nashi, connective)
                or sumu['lemma'] not in ('済む', 'すむ')
                or sumu['pos'][:1] != ['動詞']
                or not adjacent(connective, sumu)):
            continue
        end = sumu_end(tokens, i + 2)
        if end is not None:
            yield nashi['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nashidesumu', expression=NASHIDESUMU_EXPRESSION,
                   reading='なしですむ', meaning='manage without; avoid needing',
                   match=match_nashidesumu, order=117)
RULES = (RULE,)
