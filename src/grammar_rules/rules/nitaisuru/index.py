"""Attributive nominal + に対する + noun."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitaisuru(tokens):
    for i in range(1, len(tokens) - 2):
        nominal, ni, verb, noun = tokens[i - 1:i + 3]
        if (nominal['pos'][:1] not in (['名詞'], ['代名詞'])
                and not (nominal['surface'] == 'の'
                         and nominal['pos'][:2] == ['助詞', '準体助詞'])):
            continue
        if (ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('対する', 'たいする')
                and verb['lemma'] in ('対する', 'たいする')
                and verb['pos'][:1] == ['動詞']
                and noun['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in zip(
                    (nominal, ni, verb), (ni, verb, noun)))):
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=7, id='nitaisuru', expression='〜に対する',
                   reading='にたいする', meaning='toward; regarding (attributive)',
                   match=match_nitaisuru)
RULES = (RULE,)
