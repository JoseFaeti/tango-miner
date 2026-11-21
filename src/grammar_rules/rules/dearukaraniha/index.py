"""Consequential であるからには following a formal copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.dearu.index import match_dearu
from src.grammar_rules.rules.karaniha.index import match_karaniha


def match_dearukaraniha(tokens):
    copulas = set(match_dearu(tokens))
    endings = {start: end for start, end in match_karaniha(tokens)}
    for de, aru, kara in zip(tokens, tokens[1:], tokens[2:]):
        if ((de['span'][0], aru['span'][1]) in copulas
                and aru['surface'] == 'ある' and adjacent(aru, kara)
                and kara['span'][0] in endings):
            yield de['span'][0], endings[kara['span'][0]]


RULE = GrammarRule(id='dearukaraniha', expression='〜であるからには',
                   reading='であるからには', meaning='given that; since it is',
                   match=match_dearukaraniha)
RULES = (RULE,)
