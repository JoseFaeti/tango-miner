"""Conditional plus past よい, often expressing regret."""
BAYOKATTA_TARAYOKATTA_EXPRESSION = '〜ばよかった / 〜たらよかった'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches
from src.grammar_rules.rules.ba.index import match_ba
from src.grammar_rules.rules.tara.index import match_tara


def match_bayokatta_tarayokatta(tokens):
    conditionals = set(match_ba(tokens)) | set(match_tara(tokens))
    for i in range(len(tokens) - 2):
        conditional, adjective = tokens[i:i + 2]
        if (tuple(conditional['span']) in conditionals and adjacent(conditional, adjective)
                and adjective['pos'][:1] == ['形容詞']
                and adjective['lemma'] in ('いい', 'よい', '良い')
                and adjective['surface'] in ('よかっ', '良かっ')
                and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            yield conditional['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(order=25, id='bayokatta_tarayokatta',
                   expression=BAYOKATTA_TARAYOKATTA_EXPRESSION,
                   reading='ばよかった / たらよかった',
                   meaning='should have; wish one had',
                   match=match_bayokatta_tarayokatta)

RULES = (RULE,)
