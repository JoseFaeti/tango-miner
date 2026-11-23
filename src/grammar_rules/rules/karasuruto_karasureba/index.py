"""Judgment from a standpoint expressed by からすると or からすれば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜からすると / 〜からすれば'


def match_karasuruto_karasureba(tokens):
    for noun, kara, suru, ending in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and all(adjacent(a, b) for a, b in ((noun, kara), (kara, suru), (suru, ending)))
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']
                and suru['lemma'] == 'する' and suru['pos'][:1] == ['動詞']
                and (suru['surface'], ending['surface']) in (('する', 'と'), ('すれ', 'ば'))
                and ending['lemma'] == ending['surface']
                and ending['pos'][:2] == ['助詞', '接続助詞']):
            yield kara['span'][0], ending['span'][1]


RULE = GrammarRule(id='karasuruto_karasureba', expression=EXPRESSION,
                   reading='からすると / からすれば', meaning='from the standpoint of; judging from',
                   match=match_karasuruto_karasureba, order=107)
RULES = (RULE,)
