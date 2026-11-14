"""Outward signs of a feeling or desire expressed by 〜がる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches, polite_suffix_end

GARU_GRAMMAR_EXPRESSION = '〜がる'


def match_garu(tokens):
    for i in range(1, len(tokens)):
        stem, suffix = tokens[i - 1:i + 1]
        pos = stem['pos']
        adjective = (pos[:1] == ['形容詞'] and len(pos) >= 6
                     and pos[5].startswith('語幹'))
        nominal = pos[:1] == ['形状詞'] or pos[:3] == ['名詞', '普通名詞', '形状詞可能']
        if not ((adjective or nominal) and adjacent(stem, suffix)
                and suffix['lemma'] == 'がる' and suffix['pos'][:2] == ['接尾辞', '動詞的']):
            continue
        surface = suffix['surface']
        end = None
        if surface == 'がる':
            end = i
        elif surface == 'がっ':
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
        elif surface == 'がら' and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface == 'がり' and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1, forms=('positive', 'negative'))
        if end is not None:
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=130, id='garu', expression=GARU_GRAMMAR_EXPRESSION,
                   reading='がる', meaning='show signs of feeling or wanting', match=match_garu)
RULES = (RULE,)
