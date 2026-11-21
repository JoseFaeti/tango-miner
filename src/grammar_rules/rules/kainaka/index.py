"""Formal embedded whether questions with か否か."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KAINAKA_EXPRESSION = '〜か否か'


def match_kainaka(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ka, ina, final_ka = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'], ['助動詞'], ['名詞'], ['代名詞'])
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 2))
                or ka['surface'] != 'か' or ka['pos'][:1] != ['助詞']
                or ina['surface'] != '否' or ina['lemma'] != '否' or ina['pos'][:1] != ['名詞']
                or final_ka['surface'] != 'か' or final_ka['pos'][:1] != ['助詞']):
            continue
        yield ka['span'][0], final_ka['span'][1]


RULE = GrammarRule(order=37, id='kainaka', expression=KAINAKA_EXPRESSION,
                   reading='かいなか', meaning='whether or not', match=match_kainaka)
RULES = (RULE,)
