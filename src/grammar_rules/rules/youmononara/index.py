"""Hypothetical consequences after a volitional verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

YOUMONONARA_EXPRESSION = '〜ようものなら'


def match_youmononara(tokens):
    for i in range(1, len(tokens) - 2):
        verb, mono, nara = tokens[i - 1:i + 2]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('意志推量形')
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or nara['surface'] != 'なら' or nara['lemma'] != 'だ'
                or nara['pos'][:1] != ['助動詞'] or len(nara['pos']) < 6
                or not nara['pos'][5].startswith('仮定形')
                or not adjacent(verb, mono) or not adjacent(mono, nara)):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 2:]):
            continue
        yield verb['span'][0], nara['span'][1]


RULE = GrammarRule(order=136, id='youmononara', expression=YOUMONONARA_EXPRESSION,
                   reading='ようものなら',
                   meaning='if one were to; if such a thing happened (with serious consequences)',
                   match=match_youmononara)
RULES = (RULE,)
