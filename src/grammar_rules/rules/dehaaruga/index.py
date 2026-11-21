"""Concessive copular ではあるが."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

DEHAARUGA_EXPRESSION = '〜ではあるが'


def match_dehaaruga(tokens):
    for i in range(1, len(tokens)):
        previous, first = tokens[i - 1:i + 1]
        if (not adjacent(previous, first) or previous['pos'][:1] not in
                (['名詞'], ['代名詞'], ['形状詞'], ['助動詞'])):
            continue
        if (first['surface'] == 'で' and first['lemma'] in ('で', 'だ')
                and first['pos'][:1] in (['助詞'], ['助動詞'])
                and next_matches(tokens, i, 'は', '助詞', 'は')):
            end = i + 1
        elif (first['surface'] == 'じゃ' and first['lemma'] == 'だ'
              and first['pos'][:1] == ['助動詞']):
            end = i
        else:
            continue
        if (next_matches(tokens, end, 'ある', '動詞', 'ある')
                and next_matches(tokens, end + 1, 'が', '助詞', 'が')
                and tokens[end + 2]['pos'][:2] == ['助詞', '接続助詞']):
            yield first['span'][0], tokens[end + 2]['span'][1]


RULE = GrammarRule(id='dehaaruga', expression=DEHAARUGA_EXPRESSION,
                   reading='ではあるが', meaning='although; admittedly ... but',
                   match=match_dehaaruga, order=30)
RULES = (RULE,)
