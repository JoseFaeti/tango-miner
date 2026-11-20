"""Repeated concessive copulas introducing alternatives."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.deare_dearouto.index import match_deare_dearouto


def match_dearedeare(tokens):
    # The shared matcher also recognizes であろうと; retain only で + あれ.
    endings = {start: end for start, end in match_deare_dearouto(tokens)
               if end - start == 3}
    matched = set()
    for i, token in enumerate(tokens):
        start = token['span'][0]
        if start not in endings:
            continue
        j = i + 2
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        if j < len(tokens) and tokens[j]['surface'] in ('、', '，', ','):
            j += 1
            if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
                continue
        first_noun = j
        while j < len(tokens) and tokens[j]['pos'][:1] == ['名詞']:
            if j > first_noun and not adjacent(tokens[j - 1], tokens[j]):
                break
            j += 1
        if (j == first_noun or j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j])
                or tokens[j]['span'][0] not in endings):
            continue
        matched.update((start, tokens[j]['span'][0]))
    for start in sorted(matched):
        yield start, endings[start]


RULE = GrammarRule(order=1, id='dearedeare', expression='〜であれ〜であれ',
                   reading='であれであれ', meaning='whether ... or ...', match=match_dearedeare)
RULES = (RULE,)
