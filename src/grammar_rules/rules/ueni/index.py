"""Additive predicate-attached 上に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

UENI_EXPRESSION = '〜上に'


def match_ueni(tokens):
    for i in range(1, len(tokens) - 2):
        previous, noun, particle = tokens[i - 1:i + 2]
        if (noun['surface'] not in ('上', 'うえ') or noun['lemma'] != '上'
                or noun['pos'][:1] != ['名詞']
                or particle['surface'] != 'に' or particle['lemma'] != 'に'
                or particle['pos'][:2] != ['助詞', '格助詞']
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith(('連体形', '終止形'))
                or not adjacent(previous, noun) or not adjacent(noun, particle)):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 2:]):
            continue
        yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=161, id='ueni', expression=UENI_EXPRESSION,
                   reading='うえに', meaning='in addition; moreover', match=match_ueni)
RULES = (RULE,)
