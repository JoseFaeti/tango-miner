"""Concessive にしたところで, including ambiguous literal readings."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitatokorode(tokens):
    for i in range(1, len(tokens) - 4):
        previous, ni, shi, ta, tokoro, de = tokens[i - 1:i + 5]
        if (previous['pos'][:1] not in (['補助記号'], ['空白'])
                and [(t['surface'], t['lemma'], t['pos'][:1])
                     for t in (ni, shi, ta, de)] ==
                    [('に', 'に', ['助詞']), ('し', 'する', ['動詞']),
                     ('た', 'た', ['助動詞']), ('で', 'で', ['助詞'])]
                and tokoro['surface'] in ('ところ', '所')
                and tokoro['lemma'] in ('ところ', '所')
                and tokoro['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in
                        zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            yield ni['span'][0], de['span'][1]


RULE = GrammarRule(order=104, id='nishitatokorode', expression='〜にしたところで',
                   reading='にしたところで', meaning='even if; even assuming',
                   match=match_nishitatokorode)
RULES = (RULE,)
