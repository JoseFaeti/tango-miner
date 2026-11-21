"""Noun or pronoun + にしたら / にすれば: someone's perspective."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nishitara_nisureba(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, verb, ending = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or (ni['surface'], ni['lemma'], ni['pos'][:1]) != ('に', 'に', ['助詞'])
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, verb), (ni, verb, ending)))):
            continue
        if ((verb['surface'], ending['surface'], ending['lemma'], ending['pos'][:1])
                in (('し', 'たら', 'た', ['助動詞']),
                    ('すれ', 'ば', 'ば', ['助詞']))):
            yield ni['span'][0], ending['span'][1]


RULE = GrammarRule(order=12, id='nishitara_nisureba',
                   expression='〜にしたら / 〜にすれば', reading='にしたら / にすれば',
                   meaning='from the perspective of; for', match=match_nishitara_nisureba)
RULES = (RULE,)
