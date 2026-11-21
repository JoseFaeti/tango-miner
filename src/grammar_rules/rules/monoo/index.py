"""Clause-final regret or concession ものを."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monoo(tokens):
    for i in range(1, len(tokens) - 2):
        predicate, mono, particle, boundary = tokens[i - 1:i + 3]
        if (predicate["pos"][:1] not in (["動詞"], ["形容詞"], ["助動詞"])
                or len(predicate["pos"]) < 6
                or not predicate["pos"][5].startswith(("連体形", "終止形"))
                or mono["surface"] != "もの" or mono["lemma"] != "もの"
                or mono["pos"][:1] != ["名詞"]
                or particle["surface"] != "を" or particle["pos"][:2] != ["助詞", "格助詞"]
                or boundary["surface"] not in ("、", "。", "！", "？", "!", "?")
                or not adjacent(predicate, mono) or not adjacent(mono, particle)
                or not adjacent(particle, boundary)):
            continue
        yield mono["span"][0], particle["span"][1]


RULE = GrammarRule(id="monoo", expression="〜ものを", reading="ものを",
                   meaning="although; if only; and yet", match=match_monoo)
RULES = (RULE,)
