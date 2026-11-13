"""Consequential からには following a finite predicate."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.kara_node.index import match_kara_node


KARANIHA_EXPRESSION = '〜からには'


def match_karaniha(tokens):
    causal_spans = set(match_kara_node(tokens))
    for kara, ni, wa in zip(tokens, tokens[1:], tokens[2:]):
        if (tuple(kara['span']) in causal_spans and kara['surface'] == 'から'
                and adjacent(kara, ni) and adjacent(ni, wa)
                and ni['surface'] == ni['lemma'] == 'に'
                and ni['pos'][:2] == ['助詞', '格助詞']
                and wa['surface'] == wa['lemma'] == 'は'
                and wa['pos'][:2] == ['助詞', '係助詞']):
            yield kara['span'][0], wa['span'][1]


RULE = GrammarRule(id='karaniha', expression=KARANIHA_EXPRESSION,
                   reading='からには', meaning='now that; since (this is the case)',
                   match=match_karaniha, order=107)
RULES = (RULE,)
