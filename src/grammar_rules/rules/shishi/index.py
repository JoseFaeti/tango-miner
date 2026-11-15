"""Connective し following a complete predicate."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SHISHI_EXPRESSION = '〜し〜し'


def match_shishi(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (particle['surface'] == particle['lemma'] == 'し'
                and particle['pos'][:2] == ['助詞', '接続助詞']
                and adjacent(previous, particle)
                and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith('終止形')):
            yield tuple(particle['span'])


RULES = (GrammarRule(id='shishi', expression=SHISHI_EXPRESSION, reading='しし',
                     meaning='and; besides; giving reasons or examples',
                     match=match_shishi),)
