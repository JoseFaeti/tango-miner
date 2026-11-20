"""Noun + ずくめ, consisting entirely of something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ZUKUME_EXPRESSION = '〜ずくめ'


def match_zukume(tokens):
    for i in range(1, len(tokens)):
        noun, suffix = tokens[i - 1:i + 1]
        if (noun['pos'][:1] == ['名詞'] and adjacent(noun, suffix)
                and suffix['surface'] == suffix['lemma']
                and suffix['surface'] in ('ずくめ', '尽くめ')
                and suffix['pos'][:2] == ['接尾辞', '名詞的']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='zukume', expression=ZUKUME_EXPRESSION,
                   reading='ずくめ', meaning='entirely; nothing but',
                   match=match_zukume)
RULES = (RULE,)
