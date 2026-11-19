"""Continuative どころではなく construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.dokorodehanai.index import match_dokorodehanai

DOKORODEHANAKU_EXPRESSION = '〜どころではなく'


def match_dokorodehanaku(tokens):
    for start, end in match_dokorodehanai(tokens):
        for i, token in enumerate(tokens):
            if token['span'][1] != end:
                continue
            if token['surface'] != 'なく' or token['lemma'] != 'ない':
                break
            if (i + 1 < len(tokens) and adjacent(token, tokens[i + 1])
                    and tokens[i + 1]['lemma'] in ('て', 'なる')):
                break
            yield start, end
            break


RULE = GrammarRule(order=2, id='dokorodehanaku', expression=DOKORODEHANAKU_EXPRESSION,
                   reading='どころではなく', meaning='far from; rather than',
                   match=match_dokorodehanaku)
RULES = (RULE,)
