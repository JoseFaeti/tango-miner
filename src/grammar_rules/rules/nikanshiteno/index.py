"""Noun-attached attributive marker に関しての."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikanshiteno(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                and noun['pos'][:2] != ['接尾辞', '名詞的']):
            continue
        if (ni['surface'] != 'に' or ni['pos'][:2] != ['助詞', '格助詞']
                or (verb['surface'], verb['lemma']) != ('関し', '関する')
                or verb['pos'][:1] != ['動詞']
                or te['pos'][:2] != ['助詞', '接続助詞']):
            continue
        tail = tokens[i + 2:i + 4]
        if (te['surface'] != 'て' or len(tail) != 2
                or tail[1]['surface'] != 'の'
                or tail[1]['pos'][:2] != ['助詞', '格助詞']):
            continue
        phrase = [noun, ni, verb] + tail
        if all(adjacent(a, b) for a, b in pairwise(phrase)):
            yield ni['span'][0], tail[-1]['span'][1]


RULE = GrammarRule(id='nikanshiteno', expression='〜に関しての',
                   reading='にかんしての', meaning='concerning; regarding',
                   match=match_nikanshiteno, order=7)
RULES = (RULE,)
