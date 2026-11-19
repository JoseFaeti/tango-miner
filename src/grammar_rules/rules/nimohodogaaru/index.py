"""Exasperation at an excessive degree: 〜にもほどがある."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMOHODOGAARU_EXPRESSION = '〜にもほどがある'


def match_nimohodogaaru(tokens):
    for i in range(1, len(tokens) - 4):
        previous, ni, mo, hodo, ga, aru = tokens[i - 1:i + 5]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or ni['surface'] != 'に'
                or not (ni['pos'][:1] == ['助詞'] or
                        (ni['pos'][:1] == ['助動詞'] and ni['lemma'] == 'だ'))
                or (mo['surface'], mo['pos'][:1]) != ('も', ['助詞'])
                or hodo['surface'] not in ('ほど', '程')
                or hodo['pos'][:1] != ['名詞']
                or (ga['surface'], ga['pos'][:1]) != ('が', ['助詞'])
                or aru['lemma'] not in ('ある', '有る')
                or aru['pos'][:1] != ['動詞']
                or aru['surface'] not in ('ある', 'あっ', 'あり')
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, mo, hodo, ga), (ni, mo, hodo, ga, aru)))):
            continue
        end = i + 4
        if aru['surface'] in ('あっ', 'あり'):
            if end + 1 >= len(tokens) or not adjacent(aru, tokens[end + 1]):
                continue
            tail = tokens[end + 1]
            if (aru['surface'] == 'あっ' and tail['surface'] == 'た'
                    and tail['pos'][:1] == ['助動詞']):
                end += 1
            elif (aru['surface'] == 'あり' and tail['surface'] in ('ます', 'まし')
                  and tail['lemma'] == 'ます' and tail['pos'][:1] == ['助動詞']):
                end += 1
                if tail['surface'] == 'まし':
                    if (end + 1 >= len(tokens) or not adjacent(tail, tokens[end + 1])
                            or tokens[end + 1]['surface'] != 'た'
                            or tokens[end + 1]['pos'][:1] != ['助動詞']):
                        continue
                    end += 1
            else:
                continue
        yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=1, id='nimohodogaaru', expression=NIMOHODOGAARU_EXPRESSION,
                   reading='にもほどがある', meaning='there is a limit to how excessive something can be',
                   match=match_nimohodogaaru)
RULES = (RULE,)
