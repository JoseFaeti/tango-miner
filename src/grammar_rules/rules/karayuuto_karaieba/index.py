"""Judgment from a standpoint expressed by から言うと or から言えば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜から言うと / 〜から言えば'


def match_karayuuto_karaieba(tokens):
    for noun, kara, iu, ending in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and all(adjacent(a, b) for a, b in ((noun, kara), (kara, iu), (iu, ending)))
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']
                and iu['lemma'] in ('言う', 'いう') and iu['pos'][:1] == ['動詞']
                and (iu['surface'], ending['surface']) in (
                    ('言う', 'と'), ('いう', 'と'), ('言え', 'ば'), ('いえ', 'ば'))
                and ending['lemma'] == ending['surface']
                and ending['pos'][:2] == ['助詞', '接続助詞']):
            yield kara['span'][0], ending['span'][1]


RULE = GrammarRule(id='karayuuto_karaieba', expression=EXPRESSION,
                   reading='からいうと / からいえば',
                   meaning='from the standpoint of; judging from',
                   match=match_karayuuto_karaieba, order=107)
RULES = (RULE,)
