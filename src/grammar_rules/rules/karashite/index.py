"""Judgment or emphasis expressed by からして."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


KARASHITE_EXPRESSION = '〜からして'


def match_karashite(tokens):
    for noun, kara, shi, te in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and all(adjacent(a, b) for a, b in ((noun, kara), (kara, shi), (shi, te)))
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']
                and shi['surface'] == 'し' and shi['lemma'] == 'する'
                and shi['pos'][:1] == ['動詞']
                and te['surface'] == te['lemma'] == 'て'
                and te['pos'][:2] == ['助詞', '接続助詞']):
            yield kara['span'][0], te['span'][1]


RULE = GrammarRule(id='karashite', expression=KARASHITE_EXPRESSION,
                   reading='からして', meaning='judging from; even',
                   match=match_karashite, order=107)
RULES = (RULE,)
