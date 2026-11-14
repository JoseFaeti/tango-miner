"""Source-aligned case particle で."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_de(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'で' or particle['lemma'] != 'で'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=53, id='de', expression='〜で', reading='で',
                   meaning='place, means, or cause marker', match=match_de)
RULES = (RULE,)
