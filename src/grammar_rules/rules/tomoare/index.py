"""Source-aligned concessive ともあれ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOMOARE_EXPRESSION = '〜ともあれ'


def match_tomoare(tokens):
    for i, token in enumerate(tokens):
        if (token['surface'] != 'ともあれ' or token['lemma'] != 'ともあれ'
                or token['pos'][:1] != ['副詞']):
            continue
        # Sudachi can split the unrelated conditional 友あれば this way.
        if (i + 1 < len(tokens) and tokens[i + 1]['surface'] == 'ば'
                and adjacent(token, tokens[i + 1])):
            continue
        yield token['span'][0], token['span'][1]


RULE = GrammarRule(order=2, id='tomoare', expression=TOMOARE_EXPRESSION,
                   reading='ともあれ', meaning='be that as it may; in any case',
                   match=match_tomoare)
RULES = (RULE,)
