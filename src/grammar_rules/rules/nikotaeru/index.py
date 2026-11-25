"""Noun or pronoun + に応える: meet expectations or requests."""

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    negative_predicate_end,
    positive_verb_end,
)

NIKOTAERU_EXPRESSION = "〜に応える"


def match_nikotaeru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1 : i + 2]
        if (
            noun["pos"][:1] not in (["名詞"], ["代名詞"])
            or ni["surface"] != "に"
            or ni["pos"][:1] != ["助詞"]
            or verb["lemma"] not in ("応える", "こたえる")
            or verb["pos"][:1] != ["動詞"]
            or not adjacent(noun, ni)
            or not adjacent(ni, verb)
        ):
            continue
        stems = ("応え", "こたえ")
        end = positive_verb_end(
            tokens,
            i + 1,
            lemma=verb["lemma"],
            base_forms=("応える", "こたえる"),
            past_stems=stems,
            polite_stems=stems,
        )
        if end is None:
            end = negative_predicate_end(
                tokens,
                i + 1,
                polite_lemma=verb["lemma"],
                polite_stems=stems,
                plain_lemma=verb["lemma"],
                plain_stems=stems,
            )
        if end is not None:
            yield ni["span"][0], tokens[end]["span"][1]


RULE = GrammarRule(
    order=6,
    id="nikotaeru",
    expression=NIKOTAERU_EXPRESSION,
    reading="にこたえる",
    meaning="meet expectations; respond to requests",
    match=match_nikotaeru,
)

RULES = (RULE,)
