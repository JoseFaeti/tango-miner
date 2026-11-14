"""Conditional ものなら following an attributive verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


MONONARA_EXPRESSION = '〜ものなら'


def match_mononara(tokens):
    for i in range(1, len(tokens) - 2):
        verb, mono, nara = tokens[i - 1:i + 2]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('連体形')
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or nara['surface'] != 'なら' or nara['lemma'] != 'だ'
                or nara['pos'][:1] != ['助動詞'] or len(nara['pos']) < 6
                or not nara['pos'][5].startswith('仮定形')
                or not adjacent(verb, mono) or not adjacent(mono, nara)):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 2:]):
            continue
        yield mono['span'][0], nara['span'][1]


RULE = GrammarRule(order=136, id='mononara', expression=MONONARA_EXPRESSION,
                   reading='ものなら', meaning='if it were possible; if one could',
                   match=match_mononara)

RULES = (RULE,)
