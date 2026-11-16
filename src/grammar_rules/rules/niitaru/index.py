"""〜に至る: reach a result, stage, or endpoint."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIITARU_EXPRESSION = '〜に至る'


def match_niitaru(tokens):
    for i in range(1, len(tokens) - 1):
        preceding, ni, verb = tokens[i - 1:i + 2]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'], ['助動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('至る', 'いたる') or verb['pos'][:1] != ['動詞']
                or not adjacent(preceding, ni) or not adjacent(ni, verb)):
            continue
        end = i + 1
        following = tokens[end + 1] if end + 1 < len(tokens) and adjacent(verb, tokens[end + 1]) else None
        if verb['surface'] in ('至る', 'いたる'):
            if following and following['surface'] == 'まで' and following['pos'][:1] == ['助詞']:
                continue
        elif verb['surface'] in ('至っ', 'いたっ'):
            if not following or following['surface'] != 'た' or following['pos'][:1] != ['助動詞']:
                continue
            end += 1
        elif verb['surface'] in ('至り', 'いたり'):
            if not following or following['surface'] not in ('ます', 'ました') or following['pos'][:1] != ['助動詞']:
                continue
            end += 1
            if (tokens[end]['surface'] == 'ます' and end + 1 < len(tokens)
                    and adjacent(tokens[end], tokens[end + 1])
                    and tokens[end + 1]['surface'] == 'た' and tokens[end + 1]['pos'][:1] == ['助動詞']):
                end += 1
        else:
            continue
        yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=3, id='niitaru', expression=NIITARU_EXPRESSION,
                   reading='にいたる', meaning='come to; reach a point or result',
                   match=match_niitaru)
RULES = (RULE,)
