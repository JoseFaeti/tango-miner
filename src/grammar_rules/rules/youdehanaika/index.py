"""Volitional invitation with ではないか / じゃないか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

YOUDEHANAIKA_EXPRESSION = '〜ようではないか'


def match_youdehanaika(tokens):
    for i in range(len(tokens) - 1):
        verb, copula = tokens[i:i + 2]
        pos = verb['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith('意志推量形')
                or not adjacent(verb, copula) or copula['lemma'] != 'だ'
                or copula['pos'][:1] != ['助動詞']):
            continue
        if copula['surface'] == 'で' and next_matches(tokens, i + 1, 'は', '助詞', 'は'):
            end = i + 2
        elif copula['surface'] == 'じゃ':
            end = i + 1
        else:
            continue
        if next_matches(tokens, end, 'ない', '形容詞', 'ない'):
            end += 1
        elif (next_matches(tokens, end, 'ある', '動詞', 'あり')
              and next_matches(tokens, end + 1, 'ます', '助動詞', 'ませ')
              and next_matches(tokens, end + 2, 'ぬ', '助動詞', 'ん')):
            end += 3
        else:
            continue
        if (next_matches(tokens, end, 'か', '助詞', 'か')
                and tokens[end + 1]['pos'][:2] == ['助詞', '終助詞']):
            yield copula['span'][0], tokens[end + 1]['span'][1]


RULE = GrammarRule(id='youdehanaika', expression=YOUDEHANAIKA_EXPRESSION,
                   reading='ようではないか', meaning='let us; why do we not (invitation)',
                   match=match_youdehanaika, order=30)
RULES = (RULE,)
