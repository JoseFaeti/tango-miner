"""Attributive noun + に向けた: aimed at or intended for."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMUKETA_EXPRESSION = '〜に向けた'


def match_nimuketa(tokens):
    for i in range(1, len(tokens) - 3):
        target, ni, verb, ta, following = tokens[i - 1:i + 4]
        if (target['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and (verb['surface'], verb['lemma']) in {('向け', '向ける'), ('むけ', 'むける')}
                and verb['pos'][:1] == ['動詞']
                and ta['surface'] == 'た' and ta['pos'][:1] == ['助動詞']
                and following['pos'][:1] in (['名詞'], ['代名詞'], ['形容詞'], ['連体詞'])
                and all(adjacent(a, b) for a, b in zip(
                    (target, ni, verb, ta), (ni, verb, ta, following)))):
            yield ni['span'][0], ta['span'][1]


RULE = GrammarRule(order=5, id='nimuketa', expression=NIMUKETA_EXPRESSION,
                   reading='にむけた', meaning='aimed at; intended for', match=match_nimuketa)
RULES = (RULE,)
