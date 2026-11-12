"""Embedded whether questions with かどうか."""
from src.GrammarConstants import KADOUKA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kadouka(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ka, dou, final_ka = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'], ['助動詞'], ['名詞'], ['代名詞'])
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 2))
                or ka['surface'] != 'か' or ka['pos'][:1] != ['助詞']
                or dou['surface'] not in ('どう', '如何') or dou['lemma'] not in ('どう', '如何')
                or dou['pos'][:1] != ['副詞']
                or final_ka['surface'] != 'か' or final_ka['pos'][:1] != ['助詞']):
            continue
        yield ka['span'][0], final_ka['span'][1]


RULE = GrammarRule(id='kadouka', expression=KADOUKA_EXPRESSION,
                   reading='かどうか', meaning='whether or not', match=match_kadouka)
