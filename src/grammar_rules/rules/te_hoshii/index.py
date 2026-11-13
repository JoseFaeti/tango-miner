"""Desire for another person to perform an action."""
TE_HOSHII_GRAMMAR_EXPRESSION = '〜てほしい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches


def match_te_hoshii(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, desire = tokens[i - 1:i + 2]
        if connective['surface'] not in ('て', 'で') or desire['lemma'] not in ('欲しい', 'ほしい'):
            continue
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, desire)):
            continue
        lemma = desire['lemma']
        if lemma not in ('欲しい', 'ほしい'):
            continue
        stem = '欲し' if desire['surface'].startswith('欲し') else 'ほし'
        end = adjective_inflection_end(
            tokens, i + 1, lemma=lemma, pos='形容詞', base=stem + 'い',
            past_stem=stem + 'かっ', continuative_stem=stem + 'く',
            conditional_stem=stem + 'けれ')
        if end is None:
            continue
        if next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=115,
    id='te_hoshii', expression=TE_HOSHII_GRAMMAR_EXPRESSION,
    reading='てほしい', meaning='want someone to do', match=match_te_hoshii,
)

RULES = (RULE,)
