"""A state maintained unchanged: まま / 儘."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

MAMA_EXPRESSION = '〜まま'


def match_mama(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('まま', '儘')
                or marker['lemma'] not in ('まま', '儘')
                or marker['pos'][:1] != ['名詞']
                or not adjacent(previous, marker)):
            continue
        # Attributive predicates and nominal/adjectival の/な modify まま.
        if (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                or (previous['surface'] in ('の', 'な')
                    and previous['pos'][:1] in (['助詞'], ['助動詞']))):
            yield tuple(marker['span'])


RULE = GrammarRule(order=100, id='mama', expression=MAMA_EXPRESSION,
                   reading='まま', meaning='remaining in the same state; as is',
                   match=match_mama)
RULES = (RULE,)
