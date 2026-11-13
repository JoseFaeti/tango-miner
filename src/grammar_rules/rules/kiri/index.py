"""Limiting and continuing-state uses of the particle kiri."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KIRI_EXPRESSION = '〜きり'


def match_kiri(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] not in ('きり', 'っきり')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)):
            continue
        if (previous['pos'][:1] in (['名詞'], ['代名詞'])
                or (previous['surface'] in ('た', 'だ')
                    and previous['lemma'] == 'た'
                    and previous['pos'][:1] == ['助動詞'])):
            yield tuple(particle['span'])


RULE = GrammarRule(id='kiri', expression=KIRI_EXPRESSION, reading='きり',
                   meaning='only; since then, with the state continuing',
                   match=match_kiri)
RULES = (RULE,)
