"""Source-aligned case particle に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ni(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'に' or particle['lemma'] != 'に'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=53, id='ni', expression='〜に', reading='に',
                   meaning='time, destination, location, or recipient marker', match=match_ni)
RULES = (RULE,)
