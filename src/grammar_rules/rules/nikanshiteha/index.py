"""Noun-attached contrastive topic marker に関しては."""
from itertools import pairwise

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikanshiteha(tokens):
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
        tail = [te]
        if te['surface'] == 'て':
            tail = tokens[i + 2:i + 4]
            if (len(tail) != 2 or tail[1]['surface'] != 'は'
                    or tail[1]['pos'][:2] != ['助詞', '係助詞']):
                continue
        elif te['surface'] != 'ては':
            continue
        phrase = [noun, ni, verb] + tail
        if all(adjacent(a, b) for a, b in pairwise(phrase)):
            yield ni['span'][0], tail[-1]['span'][1]


RULE = GrammarRule(id='nikanshiteha', expression='〜に関しては',
                   reading='にかんしては', meaning='as for; concerning; regarding',
                   match=match_nikanshiteha, order=7)
RULES = (RULE,)
