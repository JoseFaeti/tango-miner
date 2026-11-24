"""Topic-marked noun + に対しては / に対しましては."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitaishiteha(tokens):
    for i in range(1, len(tokens) - 2):
        nominal, ni, verb = tokens[i - 1:i + 2]
        if (nominal['pos'][:1] not in (['名詞'], ['代名詞'])
                and not (nominal['surface'] == 'の'
                         and nominal['pos'][:2] == ['助詞', '準体助詞'])):
            continue
        if (ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] != '対し' or verb['lemma'] != '対する'
                or verb['pos'][:1] != ['動詞']):
            continue
        end = i + 2
        if (tokens[end]['surface'] == 'まし' and tokens[end]['lemma'] == 'ます'
                and tokens[end]['pos'][:1] == ['助動詞']):
            end += 1
        if end >= len(tokens):
            continue
        te = tokens[end]
        if te['pos'][:1] != ['助詞']:
            continue
        if te['surface'] == 'て':
            end += 1
            if (end >= len(tokens) or tokens[end]['surface'] != 'は'
                    or tokens[end]['pos'][:2] != ['助詞', '係助詞']):
                continue
        elif te['surface'] != 'ては':
            continue
        phrase = tokens[i - 1:end + 1]
        if all(adjacent(a, b) for a, b in pairwise(phrase)):
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nitaishiteha', expression='〜に対しては',
                   reading='にたいしては', meaning='as for; toward; in contrast to',
                   match=match_nitaishiteha, order=9)
RULES = (RULE,)
