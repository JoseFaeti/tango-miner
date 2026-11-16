"""Continuative form of the formal copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_deatsute(tokens):
    for i in range(1, len(tokens) - 2):
        previous, de, aru = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['空白'], ['補助記号'])
                and adjacent(previous, de) and adjacent(de, aru)
                and (de['surface'], de['lemma'], de['pos'][:1]) == ('で', 'だ', ['助動詞'])
                and (aru['surface'], aru['lemma'], aru['pos'][:1]) == ('あっ', 'ある', ['動詞'])
                and next_matches(tokens, i + 1, 'て', '助詞', 'て')):
            yield de['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='deatsute', expression='〜であって', reading='であって',
                   meaning='being; and is (formal copula)', match=match_deatsute)
RULES = (RULE,)
