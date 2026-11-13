"""Noun-attached non-exhaustive listing particles."""
YA_TOKA_EXPRESSION = 'や / とか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ya_toka(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['数詞'])
                or previous['lemma'] in ('なん', '何')
                or not adjacent(previous, particle)):
            continue
        if (particle['surface'] == 'や' and particle['lemma'] == 'や'
                and particle['pos'][:2] == ['助詞', '副助詞']):
            yield tuple(particle['span'])
        elif (particle['surface'] == 'と' and particle['lemma'] == 'と'
              and particle['pos'][:2] == ['助詞', '格助詞'] and i + 1 < len(tokens)):
            end = tokens[i + 1]
            if (end['surface'] == 'か' and end['lemma'] == 'か'
                    and end['pos'][:2] == ['助詞', '副助詞']
                    and adjacent(particle, end)):
                yield (particle['span'][0], end['span'][1])


RULE = GrammarRule(order=52, id='ya_toka', expression=YA_TOKA_EXPRESSION, reading='や / とか',
                   meaning='and; such things as; and so on', match=match_ya_toka)

RULES = (RULE,)
