"""Consequential である以上 following a formal copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.dearu.index import match_dearu


def match_dearuijiyou(tokens):
    copulas = set(match_dearu(tokens))
    for i in range(1, len(tokens) - 1):
        de, aru, ijou = tokens[i - 1:i + 2]
        if ((de['span'][0], aru['span'][1]) not in copulas
                or aru['surface'] != 'ある' or not adjacent(aru, ijou)
                or ijou['surface'] != '以上' or ijou['lemma'] != '以上'
                or ijou['pos'][:1] != ['名詞']):
            continue
        end = ijou['span'][1]
        if i + 2 < len(tokens):
            wa = tokens[i + 2]
            if (adjacent(ijou, wa) and wa['surface'] == wa['lemma'] == 'は'
                    and wa['pos'][:2] == ['助詞', '係助詞']):
                end = wa['span'][1]
        yield de['span'][0], end


RULE = GrammarRule(id='dearuijiyou', expression='〜である以上',
                   reading='であるいじょう', meaning='given that; since it is',
                   match=match_dearuijiyou)
RULES = (RULE,)
