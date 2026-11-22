"""Noun + にとどまらない: extending beyond the named extent."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_nitodomaranai(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, suffix = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('とどまる', '留まる', '止まる')
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, suffix)))):
            continue
        end = None
        if verb['surface'] in ('とどまら', '留まら', '止まら'):
            end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True,
                                      conditional=True, concessive=True)
        elif verb['surface'] in ('とどまり', '留まり', '止まり'):
            end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=1, id='nitodomaranai', expression='〜にとどまらない',
                   reading='にとどまらない', meaning='not limited to; extend beyond',
                   match=match_nitodomaranai)
RULES = (RULE,)
