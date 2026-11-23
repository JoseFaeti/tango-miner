"""Noun + のいかんにかかわらず: regardless of the circumstances."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.rules.ikannikakawarazu.index import match_ikannikakawarazu


def match_noikannikakawarazu(tokens):
    suffixes = dict(match_ikannikakawarazu(tokens))
    for previous, marker in itertools.pairwise(tokens):
        if (previous['surface'] == 'の' and previous['pos'][:1] == ['助詞']
                and marker['span'][0] in suffixes):
            yield previous['span'][0], suffixes[marker['span'][0]]


RULE = GrammarRule(order=1, id='noikannikakawarazu',
                   expression='〜のいかんにかかわらず', reading='のいかんにかかわらず',
                   meaning='regardless of the outcome or circumstances',
                   match=match_noikannikakawarazu)
RULES = (RULE,)
