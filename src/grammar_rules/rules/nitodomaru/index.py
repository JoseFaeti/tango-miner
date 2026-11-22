"""Noun + にとどまる: remain limited to the named extent."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitodomaru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('とどまる', '留まる', '止まる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] in ('とどまる', '留まる', '止まる'):
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] in ('とどまり', '留まり', '止まり') and i + 2 < len(tokens):
            polite = tokens[i + 2]
            if (polite['surface'] == 'ます' and polite['pos'][:1] == ['助動詞']
                    and adjacent(verb, polite)):
                yield ni['span'][0], polite['span'][1]
        elif verb['surface'] in ('とどまっ', '留まっ', '止まっ') and i + 2 < len(tokens):
            past = tokens[i + 2]
            if (past['surface'] == 'た' and past['pos'][:1] == ['助動詞']
                    and adjacent(verb, past)):
                yield ni['span'][0], past['span'][1]


RULE = GrammarRule(order=3, id='nitodomaru', expression='〜にとどまる',
                   reading='にとどまる', meaning='remain limited to',
                   match=match_nitodomaru)
RULES = (RULE,)
