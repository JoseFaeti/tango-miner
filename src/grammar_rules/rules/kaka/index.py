"""Alternative questions marked by two instances of か."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KAKA_EXPRESSION = '〜か〜か'
PREDICATES = ('動詞', '形容詞', '形状詞', '助動詞', '名詞', '代名詞')


def match_kaka(tokens):
    for i in range(1, len(tokens) - 2):
        first, ka = tokens[i - 1:i + 1]
        if (first['pos'][0] not in PREDICATES or ka['surface'] != 'か'
                or ka['pos'][:1] != ['助詞'] or not adjacent(first, ka)):
            continue
        j = i + 1
        if tokens[j]['surface'] in ('、', ',') and adjacent(ka, tokens[j]):
            j += 1
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        if tokens[j]['pos'][:1] not in [[p] for p in PREDICATES] or tokens[j]['surface'] in ('どう', '如何'):
            continue
        if (tokens[j]['surface'] == '否' and j + 1 < len(tokens)
                and tokens[j + 1]['surface'] == 'か' and tokens[j + 1]['pos'][:1] == ['助詞']
                and adjacent(tokens[j], tokens[j + 1])):
            continue
        j += 1
        while (j < len(tokens) and adjacent(tokens[j - 1], tokens[j])
               and tokens[j]['pos'][:1] == ['助動詞']):
            j += 1
        if (j < len(tokens) and adjacent(tokens[j - 1], tokens[j])
                and tokens[j]['surface'] == 'か' and tokens[j]['pos'][:1] == ['助詞']):
            yield tuple(ka['span'])


RULE = GrammarRule(order=38, id='kaka', expression=KAKA_EXPRESSION,
                   reading='か か', meaning='whether ... or ...', match=match_kaka)
RULES = (RULE,)
