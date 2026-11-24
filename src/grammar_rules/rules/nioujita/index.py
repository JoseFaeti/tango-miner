"""Attributive noun + に応じた + noun."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nioujita(tokens):
    for i in range(1, len(tokens) - 3):
        nominal, ni, verb, past, noun = tokens[i - 1:i + 4]
        if (nominal['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('応じ', 'おうじ')
                and verb['lemma'] in ('応じる', '応ずる', 'おうじる', 'おうずる')
                and verb['pos'][:1] == ['動詞']
                and past['surface'] == 'た' and past['lemma'] == 'た'
                and past['pos'][:1] == ['助動詞']
                and noun['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in zip(
                    (nominal, ni, verb, past), (ni, verb, past, noun)))):
            yield ni['span'][0], past['span'][1]


RULE = GrammarRule(order=6, id='nioujita', expression='〜に応じた',
                   reading='におうじた', meaning='suited to; in accordance with (attributive)',
                   match=match_nioujita)
RULES = (RULE,)
