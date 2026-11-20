"""Paired positive and negative alternatives marked by が."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.mai.index import match_mai


def match_yougamaiga(tokens):
    mai_spans = list(match_mai(tokens))
    for i, first in enumerate(tokens):
        if first['surface'] == 'しよう' and first['pos'][:1] in (['名詞'], ['動詞']):
            lemma, end = 'する', i
        elif first['pos'][:1] == ['動詞'] and len(first['pos']) >= 6:
            lemma = first['lemma']
            if first['pos'][5].startswith('意志推量形'):
                end = i
            elif (i + 1 < len(tokens) and adjacent(first, tokens[i + 1])
                  and tokens[i + 1]['surface'] == 'よう'
                  and first['pos'][5].startswith(('連用形', '未然形'))):
                end = i + 1
            else:
                continue
        elif (first['surface'] == '来' and first['pos'][:1] == ['接頭辞']
              and i + 1 < len(tokens) and adjacent(first, tokens[i + 1])
              and tokens[i + 1]['surface'] == 'よう'):
            lemma, end = '来る', i + 1
        else:
            continue
        if end + 3 >= len(tokens):
            continue
        ga, verb = tokens[end + 1:end + 3]
        if (ga['surface'] != 'が' or ga['pos'][:1] != ['助詞']
                or not adjacent(tokens[end], ga) or not adjacent(ga, verb)):
            continue
        if lemma == 'する' and verb['surface'] == 'しまい' and verb['pos'][:1] == ['名詞']:
            negative_end = end + 2
        elif (verb['lemma'] == lemma and verb['pos'][:1] == ['動詞']
              and end + 3 < len(tokens) and tokens[end + 3]['span'] in mai_spans):
            negative_end = end + 3
        else:
            continue
        if negative_end + 1 >= len(tokens):
            continue
        final = tokens[negative_end + 1]
        if (final['surface'] == 'が' and final['pos'][:1] == ['助詞']
                and adjacent(tokens[negative_end], final)):
            yield first['span'][0], final['span'][1]


RULE = GrammarRule(id='yougamaiga', expression='〜ようが〜まいが',
                   reading='ようがまいが', meaning='whether or not',
                   match=match_yougamaiga, order=2)
RULES = (RULE,)
