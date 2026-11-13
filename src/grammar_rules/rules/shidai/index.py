"""Immediate succession expressed by a verbal stem or event noun plus 次第."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SHIDAI_EXPRESSION = '〜次第'


def match_shidai(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('次第', 'しだい')
                or marker['lemma'] not in ('次第', 'しだい')
                or marker['pos'][:1] != ['名詞'] or not adjacent(previous, marker)):
            continue
        verb_stem = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連用形'))
        event_noun = previous['pos'][:3] == ['名詞', '普通名詞', 'サ変可能']
        if not (verb_stem or event_noun):
            continue
        if (i + 1 < len(tokens) and adjacent(marker, tokens[i + 1])
                and tokens[i + 1]['surface'] in ('で', 'だ', 'です', 'に', 'の', 'を', 'が', 'は', 'も')):
            continue
        yield marker['span'][0], marker['span'][1]


RULE = GrammarRule(order=160, id='shidai', expression=SHIDAI_EXPRESSION,
                   reading='しだい', meaning='as soon as; immediately after',
                   match=match_shidai)
RULES = (RULE,)
