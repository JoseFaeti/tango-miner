"""Emphatic までして: going so far as to do something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_madeshite(tokens):
    for previous, made, shi, te in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (all(adjacent(a, b) for a, b in ((previous, made), (made, shi), (shi, te)))
                and made['surface'] == made['lemma'] == 'まで'
                and made['pos'][:2] == ['助詞', '副助詞']
                and shi['surface'] == 'し' and shi['lemma'] == 'する'
                and shi['pos'][:1] == ['動詞']
                and te['surface'] == te['lemma'] == 'て'
                and te['pos'][:2] == ['助詞', '接続助詞']):
            yield made['span'][0], te['span'][1]


RULE = GrammarRule(id='madeshite', expression='〜までして', reading='までして',
                   meaning='going so far as to', match=match_madeshite, order=107)
RULES = (RULE,)
