"""Hearsay or characteristic expressed by らしい."""
from src.GrammarConstants import RASHII_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches


def match_rashii(tokens):
    for i in range(1, len(tokens)):
        previous, suffix = tokens[i - 1:i + 1]
        if not adjacent(previous, suffix):
            continue
        pos = previous['pos']
        attachment = (pos[:1] in (['名詞'], ['代名詞'], ['形状詞']) or
                      (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                       and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))))
        if not attachment or suffix['pos'][:1] not in (['助動詞'], ['接尾辞']):
            continue
        if suffix['pos'][:1] == ['接尾辞'] and suffix['pos'][:2] != ['接尾辞', '形容詞的']:
            continue
        end = adjective_inflection_end(
            tokens, i, lemma='らしい', pos=suffix['pos'][0], base='らしい',
            past_stem='らしかっ', continuative_stem='らしく', conditional_stem='らしけれ')
        if end is not None:
            if next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='rashii', expression=RASHII_EXPRESSION, reading='らしい',
                   meaning='apparently; characteristic of', match=match_rashii)
