"""Polite negative and past verbal endings."""
MASEN_MASHITA_EXPRESSION = '〜ません / 〜ました / 〜ませんでした'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, polite_suffix_end


def match_masen_mashita(tokens):
    for i in range(1, len(tokens)):
        previous, suffix = tokens[i - 1:i + 1]
        if (not adjacent(previous, suffix) or previous['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')
                or suffix['surface'] not in ('ませ', 'まし', 'ません', 'ませんでした', 'ました')):
            continue
        end = polite_suffix_end(tokens, i, forms=('positive', 'negative'))
        if end is not None:
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=169, id='masen_mashita', expression=MASEN_MASHITA_EXPRESSION,
                   reading='ません / ました / ませんでした',
                   meaning='polite negative, past, or past negative verb ending',
                   match=match_masen_mashita)

RULES = (RULE,)
