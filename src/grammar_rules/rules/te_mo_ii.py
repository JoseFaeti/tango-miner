"""Verb te-form plus mo ii, expressing permission or acceptance."""
from src.GrammarConstants import TE_MO_II_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_te_mo_ii(tokens):
    for i in range(1, len(tokens)):
        previous, token = tokens[i - 1:i + 1]
        if (not adjacent(previous, token) or previous['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')):
            continue
        if token['surface'] == 'てもいい' and token['lemma'] == 'てもいい':
            yield token['span'][0], token['span'][1]
            continue
        if (token['surface'] not in ('て', 'で') or token['pos'][:2] != ['助詞', '接続助詞']
                or not next_matches(tokens, i, 'も', '助詞', 'も') or i + 2 >= len(tokens)
                or not adjacent(tokens[i + 1], tokens[i + 2])):
            continue
        adjective = tokens[i + 2]
        if adjective['pos'][:1] != ['形容詞'] or adjective['lemma'] not in ('いい', 'よい', '良い'):
            continue
        end = i + 2
        if adjective['surface'] in ('よかっ', '良かっ'):
            if not next_matches(tokens, end, 'た', '助動詞', 'た'):
                continue
            end += 1
        elif adjective['surface'] not in ('いい', 'よい', '良い'):
            continue
        yield token['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='te_mo_ii', expression=TE_MO_II_GRAMMAR_EXPRESSION, reading='てもいい',
    meaning='may; can; it is all right to; even if',
    match=match_te_mo_ii,
)
