"""Noun or pronoun + に応える: meet expectations or requests."""

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (
    adjacent,
    negative_predicate_end,
    negative_suffix_end,
    next_matches,
    polite_suffix_end,
    positive_verb_end,
)

NIKOTAERU_EXPRESSION = "〜に応える"


def auxiliary_end(tokens, index, *, lemma, stems, conditional_stems):
    if not 0 < index < len(tokens):
        return None
    auxiliary = tokens[index]
    if (auxiliary["lemma"] != lemma
            or auxiliary["pos"][:1] != ["助動詞"]
            or not adjacent(tokens[index - 1], auxiliary)):
        return None
    end = positive_verb_end(
        tokens,
        index,
        lemma=lemma,
        base_forms=(lemma,),
        past_stems=stems,
        polite_stems=stems,
        conditional_stems=conditional_stems,
        pos="助動詞",
    )
    if end is not None:
        return end
    if auxiliary["surface"] not in stems:
        return None
    if next_matches(tokens, index, "ない", "助動詞"):
        return negative_suffix_end(tokens, index + 1, pos="助動詞")
    if next_matches(tokens, index, "ます", "助動詞"):
        return polite_suffix_end(tokens, index + 1, forms=("negative",))
    if next_matches(tokens, index, "た", "助動詞", "たら"):
        return index + 1
    return None


def volitional_verb_end(tokens, index, lemma):
    verb = tokens[index]
    if verb["lemma"] != lemma or verb["pos"][:1] != ["動詞"]:
        return None
    if verb["surface"] in ("応えよう", "こたえよう"):
        return index
    if (verb["surface"] in ("応えよ", "こたえよ")
            and next_matches(tokens, index, "う", "助動詞", "う")):
        return index + 1
    return None


def negative_conditional_end(tokens, index, lemma, stems):
    verb = tokens[index]
    if (verb["lemma"] != lemma or verb["pos"][:1] != ["動詞"]
            or verb["surface"] not in stems
            or not next_matches(tokens, index, "ない", "助動詞")):
        return None
    return negative_suffix_end(tokens, index + 1, pos="助動詞", conditional=True)


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
            conditional_stems=("応えれ", "こたえれ"),
        )
        if end is None and next_matches(tokens, i + 1, "た", "助動詞", "たら"):
            end = i + 2
        if end is None:
            end = negative_predicate_end(
                tokens,
                i + 1,
                polite_lemma=verb["lemma"],
                polite_stems=stems,
                plain_lemma=verb["lemma"],
                plain_stems=stems,
            )
        if end is None:
            end = negative_conditional_end(tokens, i + 1, verb["lemma"], stems)
        if end is None:
            end = volitional_verb_end(tokens, i + 1, verb["lemma"])
        if (end is None and verb["surface"] in stems
                and next_matches(tokens, i + 1, "ます", "助動詞")):
            end = polite_suffix_end(tokens, i + 2, forms=("volitional",))
        if end is None:
            end = auxiliary_end(
                tokens,
                i + 2,
                lemma="られる",
                stems=("られ",),
                conditional_stems=("られれ",),
            )
        if end is None:
            end = auxiliary_end(
                tokens,
                i + 2,
                lemma="させる",
                stems=("させ",),
                conditional_stems=("させれ",),
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
