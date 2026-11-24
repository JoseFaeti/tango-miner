"""Noun or pronoun + に応える: meet expectations or requests."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKOTAERU_EXPRESSION = '〜に応える'


def match_nikotaeru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('応える', 'こたえる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] in ('応える', 'こたえる'):
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] in ('応え', 'こたえ') and i + 2 < len(tokens):
            tail = tokens[i + 2]
            if (tail['surface'] == 'た' and tail['pos'][:1] == ['助動詞']
                    and adjacent(verb, tail)):
                yield ni['span'][0], tail['span'][1]


RULE = GrammarRule(order=6, id='nikotaeru', expression=NIKOTAERU_EXPRESSION,
                   reading='にこたえる', meaning='meet expectations; respond to requests',
                   match=match_nikotaeru)

RULES = (RULE,)
