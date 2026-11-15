"""Formal appeal or recognition with ではないか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

DEHANAIKA_EXPRESSION = '〜ではないか'


def match_dehanaika(tokens):
    for i in range(1, len(tokens) - 1):
        previous, copula = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not adjacent(previous, copula):
            continue
        if pos[:1] in (['動詞'], ['形容詞'], ['助動詞']):
            if len(pos) < 6 or not pos[5].startswith(('終止形', '連体形')):
                continue
        elif (pos[:1] not in (['名詞'], ['代名詞'], ['形状詞'], ['副詞'])
              and not (previous['surface'] == 'の' and pos[:1] == ['助詞'])):
            continue
        if (copula['surface'] != 'で' or copula['pos'][:1] not in (['助詞'], ['助動詞'])
                or not next_matches(tokens, i, 'は', '助詞', 'は')):
            continue
        end = i + 1
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


RULE = GrammarRule(id='dehanaika', expression=DEHANAIKA_EXPRESSION,
                   reading='ではないか', meaning='recognition, emphasis, or appeal to shared understanding',
                   match=match_dehanaika, order=31)
RULES = (RULE,)
