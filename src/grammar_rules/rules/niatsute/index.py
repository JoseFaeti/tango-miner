"""Formal noun + にあって: in a situation or setting."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIATSUTE_EXPRESSION = '〜にあって'

# Kana あって is frequently analyzed as ある in these 遭う collocations.
MISFORTUNE_HEADS = frozenset(('目', '神隠し', '事故', '災難', '災害', '被害', '盗難', '詐欺'))


def is_misfortune_au(noun, ni, verb):
    return (noun['lemma'] in MISFORTUNE_HEADS and ni['surface'] == 'に'
            and verb['surface'] == 'あっ' and adjacent(noun, ni)
            and adjacent(ni, verb))


def match_niatsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and not is_misfortune_au(noun, ni, verb)
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('あっ', '有っ', '在っ')
                and verb['lemma'] in ('ある', '有る', '在る')
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=1, id='niatsute', expression=NIATSUTE_EXPRESSION,
                   reading='にあって', meaning='in the situation of; amid',
                   match=match_niatsute)
RULES = (RULE,)
