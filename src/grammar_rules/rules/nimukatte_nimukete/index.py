"""Noun + に向かって / に向けて: toward or aimed at."""
NIMUKATTE_NIMUKETE_EXPRESSION = '〜に向かって / 〜に向けて'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nimukatte_nimukete(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb, te = tokens[i - 1:i + 3]
        forms = {('向かっ', '向かう'), ('むかっ', 'むかう'),
                 ('向け', '向ける'), ('むけ', 'むける')}
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and (verb['surface'], verb['lemma']) in forms
                and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb), (ni, verb, te)))):
            yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=5, id='nimukatte_nimukete', expression=NIMUKATTE_NIMUKETE_EXPRESSION,
                   reading='にむかって / にむけて', meaning='toward; aimed at',
                   match=match_nimukatte_nimukete)

RULES = (RULE,)
