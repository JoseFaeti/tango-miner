"""〜に関わりなく: regardless of a preceding condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAKAWARINAKU_EXPRESSION = '〜に関わりなく'


def match_nikakawarinaku(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, relation, naku = tokens[i - 1:i + 3]
        if (previous['pos'][:1] == ['補助記号']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or relation['surface'] not in ('関わり', 'かかわり', '拘わり')
                or (relation['lemma'], relation['pos'][0]) not in (
                    ('関わり', '名詞'), ('かかわり', '名詞'),
                    ('拘わり', '名詞'), ('関わる', '動詞'),
                    ('かかわる', '動詞'), ('拘わる', '動詞'))
                or naku['surface'] != 'なく' or naku['lemma'] != 'ない'
                or naku['pos'][:1] != ['形容詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, relation), (ni, relation, naku)))):
            continue
        yield ni['span'][0], naku['span'][1]


RULE = GrammarRule(order=1, id='nikakawarinaku', expression=NIKAKAWARINAKU_EXPRESSION,
                   reading='にかかわりなく', meaning='regardless of; irrespective of',
                   match=match_nikakawarinaku)
RULES = (RULE,)
