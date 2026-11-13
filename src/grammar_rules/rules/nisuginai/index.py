"""Limitation expressed by にすぎない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end

NISUGINAI_EXPRESSION = '〜にすぎない'


def match_nisuginai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, sugi, suffix = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or not all(adjacent(a, b) for a, b in
                           ((previous, ni), (ni, sugi), (sugi, suffix)))
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or sugi['surface'] not in ('すぎ', '過ぎ')
                or sugi['lemma'] not in ('すぎる', '過ぎる')
                or sugi['pos'][:1] != ['動詞']):
            continue
        end = negative_suffix_end(tokens, i + 2, pos='助動詞', conditional=True)
        if suffix['surface'] == 'なく' and suffix['lemma'] == 'ない':
            if next_matches(tokens, i + 2, 'て', '助詞', 'て'):
                end = i + 3
        if suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=36, id='nisuginai', expression=NISUGINAI_EXPRESSION,
                   reading='にすぎない', meaning='merely; no more than', match=match_nisuginai)
RULES = (RULE,)
