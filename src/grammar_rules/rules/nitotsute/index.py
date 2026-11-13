"""Noun or pronoun + にとって: from the perspective of."""
from src.GrammarConstants import NITOTSUTE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitotsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != 'とる' or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        suffix = [('て', '助詞')] if verb['surface'] == 'とっ' else (
            [('まし', '助動詞'), ('て', '助詞')] if verb['surface'] == 'とり' else [])
        if not suffix or i + 2 + len(suffix) > len(tokens):
            continue
        tail = tokens[i + 2:i + 2 + len(suffix)]
        if ([(t['surface'], t['pos'][0]) for t in tail] == suffix
                and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
            yield ni['span'][0], tail[-1]['span'][1]


RULE = GrammarRule(id='nitotsute', expression=NITOTSUTE_EXPRESSION,
                   reading='にとって', meaning='for; from the perspective of',
                   match=match_nitotsute)
