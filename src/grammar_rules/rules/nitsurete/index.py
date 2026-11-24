"""Concurrent change expressed by noun or dictionary-form verb + につれて."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NITSURETE_EXPRESSION = '〜につれて'


def match_nitsurete(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, te = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (preceding['pos'][:1] == ['動詞']
                    and preceding['surface'] != preceding['lemma'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('つれ', 'つれる'), ('連れ', '連れる'))
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (preceding, ni, verb), (ni, verb, te)))):
            continue
        # A destination followed by taking someone along remains lexical.
        if (verb['surface'] == '連れ'
                and preceding['pos'][:1] != ['動詞']
                and i + 3 < len(tokens)
                and adjacent(te, tokens[i + 3])
                and tokens[i + 3]['lemma'] in ('行く', 'いく', '来る', 'くる', '帰る', '戻る')):
            continue
        yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=11, id='nitsurete', expression=NITSURETE_EXPRESSION,
                   reading='につれて', meaning='as; in proportion as',
                   match=match_nitsurete)
RULES = (RULE,)
