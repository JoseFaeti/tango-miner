"""Noun + に反する: contrary to; against."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nihansuru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != '反する' or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] == '反する':
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] == '反し' and i + 2 < len(tokens):
            polite = tokens[i + 2]
            if (polite['surface'] == 'ます' and polite['pos'][:1] == ['助動詞']
                    and adjacent(verb, polite)):
                yield ni['span'][0], polite['span'][1]


RULE = GrammarRule(order=10, id='nihansuru', expression='〜に反する',
                   reading='にはんする', meaning='contrary to; against',
                   match=match_nihansuru)

RULES = (RULE,)
