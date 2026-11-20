"""Attributive literary prohibition with verbal べからざる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.bekarazu.index import match_bekarazu


def match_bekarazaru(tokens):
    for start, end in match_bekarazu(tokens):
        if any(token["span"][1] == end and token["surface"] == "ざる"
               for token in tokens):
            yield start, end


RULE = GrammarRule(id="bekarazaru", expression="〜べからざる", reading="べからざる",
                   meaning="cannot; must not (literary attributive)",
                   match=match_bekarazaru)
RULES = (RULE,)
