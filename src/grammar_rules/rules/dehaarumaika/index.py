"""Tentative supposition with ではあるまいか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

DEHAARUMAIKA_EXPRESSION = '〜ではあるまいか'


def match_dehaarumaika(tokens):
    for i in range(1, len(tokens)):
        previous, first = tokens[i - 1:i + 1]
        if not adjacent(previous, first) or previous['pos'][:1] not in (
                ['名詞'], ['代名詞'], ['形状詞'], ['助動詞']):
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
                and next_matches(tokens, end + 1, 'まい', '助動詞', 'まい')
                and next_matches(tokens, end + 2, 'か', '助詞', 'か')
                and tokens[end + 3]['pos'][:2] == ['助詞', '終助詞']):
            yield first['span'][0], tokens[end + 3]['span'][1]


RULE = GrammarRule(id='dehaarumaika', expression=DEHAARUMAIKA_EXPRESSION,
                   reading='ではあるまいか', meaning='might it not be; perhaps',
                   match=match_dehaarumaika, order=30)
RULES = (RULE,)
