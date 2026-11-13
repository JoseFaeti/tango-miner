"""Conditional negative copula: unless; otherwise."""
JIYANAITO_DENAITO_EXPRESSION = '〜じゃないと / 〜でないと'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_jiyanaito_denaito(tokens):
    for i in range(len(tokens) - 2):
        copula, negative, particle = tokens[i:i + 3]
        if not (adjacent(copula, negative) and adjacent(negative, particle)):
            continue
        if not ((copula['surface'] == 'じゃ' and copula['lemma'] == 'だ'
                 and copula['pos'][:1] == ['助動詞'])
                or (copula['surface'] == 'で' and copula['pos'][:1] in (['助動詞'], ['助詞']))):
            continue
        standalone = (copula['surface'] == 'じゃ' and
                      (i == 0 or tokens[i - 1]['pos'][:1] == ['補助記号']))
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['形容詞'], ['助動詞'])
                or particle['surface'] != 'と'
                or (particle['pos'][:2] != ['助詞', '接続助詞']
                    and not (standalone and particle['pos'][:2] == ['助詞', '格助詞']))):
            continue
        # A preceding noun, pronoun or na-adjective supplies the copular predicate.
        # The initial connective じゃないと is also used on its own.
        if standalone:
            pass
        elif i and adjacent(tokens[i - 1], copula):
            if tokens[i - 1]['pos'][:1] not in (['名詞'], ['代名詞'], ['形状詞']):
                continue
        elif copula['surface'] != 'じゃ':
            continue
        yield copula['span'][0], particle['span'][1]


RULE = GrammarRule(order=32, id='jiyanaito_denaito', expression=JIYANAITO_DENAITO_EXPRESSION,
                   reading='じゃないと / でないと', meaning='unless; otherwise',
                   match=match_jiyanaito_denaito)

RULES = (RULE,)
