"""Attached causal ため without the separate ために construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TAME_EXPRESSION = '〜ため'


def match_tame(tokens):
    for i in range(1, len(tokens)):
        previous, tame = tokens[i - 1:i + 1]
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        nominal = (previous['surface'] == previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        demonstrative = (previous['surface'] in ('その', 'この', 'あの', 'どの')
                         and previous['pos'][:1] == ['連体詞'])
        if not ((predicate or nominal or demonstrative) and adjacent(previous, tame)):
            continue
        if (tame['surface'] not in ('ため', '為') or tame['lemma'] not in ('ため', '為')
                or tame['pos'][:1] != ['名詞']):
            continue
        # A comma marks the causal connective; other continuations can be
        # nominal uses or belong to the independently tracked ために rule.
        if (i + 1 < len(tokens) and adjacent(tame, tokens[i + 1])
                and tokens[i + 1]['surface'] in ('、', '，', ',')):
            yield tame['span'][0], tame['span'][1]


RULE = GrammarRule(order=122, id='tame', expression=TAME_EXPRESSION,
                   reading='ため', meaning='because of; due to', match=match_tame)
RULES = (RULE,)
