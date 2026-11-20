"""Repeated やら marking a non-exhaustive or uncertain enumeration."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

YARAYARA_EXPRESSION = '〜やら〜やら'


def match_yarayara(tokens):
    markers = []
    for i, token in enumerate(tokens):
        if token['surface'] in ('。', '！', '？', '!', '?', '\n'):
            markers = []
        if (i and token['surface'] == token['lemma'] == 'やら'
                and token['pos'][:2] == ['助詞', '副助詞']
                and tokens[i - 1]['pos'][:1] in (['名詞'], ['代名詞'], ['数詞'],
                                                  ['動詞'], ['形容詞'], ['形状詞'], ['助動詞'])
                and adjacent(tokens[i - 1], token)):
            if markers and i > markers[-1] + 1:
                if len(markers) == 1:
                    yield tuple(tokens[markers[0]]['span'])
                yield tuple(token['span'])
            markers.append(i)


RULE = GrammarRule(order=52, id='yarayara', expression=YARAYARA_EXPRESSION,
                   reading='やらやら', meaning='such things as; whether or what',
                   match=match_yarayara)
RULES = (RULE,)
