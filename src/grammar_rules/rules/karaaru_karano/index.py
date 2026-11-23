"""Quantity expressions からある and からの."""
import re

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KARAARU_KARANO_EXPRESSION = '〜からある / 〜からの'
_NUMBER = re.compile(r'^[0-9０-９一二三四五六七八九十百千万億兆]+')


def match_karaaru_karano(tokens):
    for i in range(1, len(tokens) - 1):
        quantity, kara, ending = tokens[i - 1:i + 2]
        if not (adjacent(quantity, kara) and adjacent(kara, ending)
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']):
            continue
        has_number = bool(_NUMBER.match(quantity['surface']))
        if not has_number and i >= 2:
            number = tokens[i - 2]
            has_number = (adjacent(number, quantity) and bool(_NUMBER.match(number['surface']))
                          and (quantity['pos'][:1] == ['接尾辞']
                               or '助数詞可能' in quantity['pos']))
        if not has_number:
            continue
        if ((ending['surface'] == 'ある' and ending['lemma'] == 'ある'
             and ending['pos'][:1] in (['動詞'], ['連体詞']))
                or (ending['surface'] == ending['lemma'] == 'の'
                    and ending['pos'][:2] == ['助詞', '格助詞'])):
            yield kara['span'][0], ending['span'][1]


RULE = GrammarRule(id='karaaru_karano', expression=KARAARU_KARANO_EXPRESSION,
                   reading='からある / からの', meaning='as many as; as much as',
                   match=match_karaaru_karano, order=55)
RULES = (RULE,)
