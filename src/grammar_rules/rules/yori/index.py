"""A conservative comparison use of the particle より."""
YORI_EXPRESSION = '〜より'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_yori(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, following = tokens[i - 1:i + 2]
        if (particle['surface'] != 'より' or particle['lemma'] != 'より'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] not in (['名詞'], ['代名詞'], ['数詞'])
                or not adjacent(previous, particle) or not adjacent(particle, following)):
            continue
        if following['pos'][:1] == ['形容詞']:
            yield tuple(particle['span'])


RULE = GrammarRule(order=56, id='yori', expression=YORI_EXPRESSION, reading='より',
                   meaning='than; compared with', match=match_yori)

RULES = (RULE,)
