"""Emphatic causal からこそ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.kara_node.index import match_kara_node


KARAKOSO_EXPRESSION = '〜からこそ'


def match_karakoso(tokens):
    causal_spans = set(match_kara_node(tokens))
    for kara, koso in zip(tokens, tokens[1:]):
        if (tuple(kara['span']) in causal_spans and kara['surface'] == 'から'
                and adjacent(kara, koso) and koso['surface'] == 'こそ'
                and koso['lemma'] == 'こそ' and koso['pos'][:2] == ['助詞', '係助詞']):
            yield kara['span'][0], koso['span'][1]


RULE = GrammarRule(id='karakoso', expression=KARAKOSO_EXPRESSION,
                   reading='からこそ', meaning='precisely because',
                   match=match_karakoso, order=107)
RULES = (RULE,)
