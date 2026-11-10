"""Resemblance or tendency expressed by the adjective-like suffix っぽい."""
from src.GrammarConstants import TSUPOI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches


def match_tsupoi(tokens):
    for i in range(1, len(tokens)):
        previous, suffix = tokens[i - 1:i + 1]
        if not adjacent(previous, suffix) or suffix['pos'][:2] != ['接尾辞', '形容詞的']:
            continue
        pos = previous['pos']
        if not (pos[:1] == ['名詞'] or
                (pos[:1] == ['動詞'] and len(pos) >= 6 and pos[5].startswith('連用形'))):
            continue
        end = adjective_inflection_end(
            tokens, i, lemma='っぽい', pos='接尾辞', base='っぽい',
            past_stem='っぽかっ', continuative_stem='っぽく', conditional_stem='っぽけれ')
        if end is not None:
            if next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tsupoi', expression=TSUPOI_EXPRESSION, reading='っぽい',
                   meaning='resembling; tending to', match=match_tsupoi)
