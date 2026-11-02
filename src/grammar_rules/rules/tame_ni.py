"""Attached ために for purpose, benefit or cause."""
from src.GrammarConstants import TAME_NI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tame_ni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, tame, ni = tokens[i - 1:i + 2]
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        if not (predicate or nominal) or not adjacent(previous, tame):
            continue
        if (tame['surface'] not in ('ため', '為') or tame['lemma'] not in ('ため', '為')
                or tame['pos'][:1] != ['名詞'] or not adjacent(tame, ni)
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:2] != ['助詞', '格助詞']):
            continue
        following = []
        for token in tokens[i + 2:]:
            if token['surface'] in ('。', '！', '？', '!', '?'):
                break
            following.append(token)
        if any(token['pos'][:1] != ['補助記号'] for token in following):
            yield tame['span'][0], ni['span'][1]


RULE = GrammarRule(
    id='tame_ni', expression=TAME_NI_GRAMMAR_EXPRESSION, reading='ために',
    meaning='for; in order to; because of', match=match_tame_ni,
)
