"""Desire for a thing, expressed by the adjective 欲しい."""
HOSHII_GRAMMAR_EXPRESSION = '〜ほしい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjective_inflection_end, next_matches
from src.grammar_rules.rules.te_hoshii.index import match_te_hoshii


def match_hoshii(tokens):
    action_spans = list(match_te_hoshii(tokens))
    for i, desire in enumerate(tokens):
        if desire['lemma'] not in ('欲しい', 'ほしい'):
            continue
        start = desire['span'][0]
        if any(a <= start < b for a, b in action_spans):
            continue
        stem = '欲し' if desire['surface'].startswith('欲し') else 'ほし'
        end = adjective_inflection_end(
            tokens, i, lemma=desire['lemma'], pos='形容詞', base=stem + 'い',
            past_stem=stem + 'かっ', continuative_stem=stem + 'く',
            conditional_stem=stem + 'けれ')
        if end is None:
            continue
        if next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=173,
    id='hoshii', expression=HOSHII_GRAMMAR_EXPRESSION, reading='ほしい',
    meaning='want; desire something', match=match_hoshii,
)

RULES = (RULE,)
