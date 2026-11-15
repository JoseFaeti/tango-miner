"""Predicate-attached connective て / で."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TE_GRAMMAR_EXPRESSION = '〜て'


def match_te(tokens):
    for i in range(1, len(tokens)):
        predicate, particle = tokens[i - 1:i + 1]
        pos = predicate['pos']
        if (particle['surface'] in ('て', 'で')
                and particle['lemma'] in ('て', 'で')
                and particle['pos'][:2] == ['助詞', '接続助詞']
                and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith('連用形')
                and adjacent(predicate, particle)):
            yield tuple(particle['span'])


RULE = GrammarRule(order=160, id='te', expression=TE_GRAMMAR_EXPRESSION,
                   reading='て', meaning='and; then; please (request)', match=match_te)
RULES = (RULE,)
