"""Noun + をはじめ: giving a leading example."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, has_next


def match_ohajime(tokens):
    for i in range(1, len(tokens) - 1):
        noun, particle, hajime = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or hajime['surface'] not in ('はじめ', '始め')
                or (hajime['lemma'], hajime['pos'][:1]) not in (
                    ('はじめ', ['名詞']), ('始め', ['名詞']), ('始める', ['動詞']),
                    ('はじめる', ['動詞']))
                or not adjacent(noun, particle) or not adjacent(particle, hajime)):
            continue
        if (has_next(tokens, i + 1)
                and (tokens[i + 2]['pos'][:1] == ['助動詞']
                     or tokens[i + 2]['surface'] in ('て', 'ながら'))):
            continue
        yield particle['span'][0], hajime['span'][1]


RULE = GrammarRule(order=7, id='ohajime', expression='〜をはじめ',
                   reading='をはじめ', meaning='including; starting with (a leading example)',
                   match=match_ohajime)
RULES = (RULE,)
