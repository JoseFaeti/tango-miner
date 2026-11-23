"""A viewpoint or judgment expressed with から見ると or から見れば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜から見ると / 〜から見れば'


def match_karamiruto_karamireba(tokens):
    for noun, kara, miru, ending in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and all(adjacent(a, b) for a, b in ((noun, kara), (kara, miru), (miru, ending)))
                and kara['surface'] == kara['lemma'] == 'から'
                and kara['pos'][:2] == ['助詞', '格助詞']
                and miru['lemma'] in ('見る', 'みる') and miru['pos'][:1] == ['動詞']
                and (miru['surface'], ending['surface']) in (
                    ('見る', 'と'), ('みる', 'と'), ('見れ', 'ば'), ('みれ', 'ば'))
                and ending['lemma'] == ending['surface']
                and ending['pos'][:2] == ['助詞', '接続助詞']):
            yield kara['span'][0], ending['span'][1]


RULE = GrammarRule(id='karamiruto_karamireba', expression=EXPRESSION,
                   reading='からみると / からみれば',
                   meaning='from the viewpoint of; judging from',
                   match=match_karamiruto_karamireba, order=107)
RULES = (RULE,)
