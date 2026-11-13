"""Predicate-attached inference that something cannot be the case."""
HAZUGANAI_EXPRESSION = '〜はずがない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end


def match_hazuganai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, hazu, ga, nai = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in ((previous, hazu), (hazu, ga), (ga, nai))):
            continue
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞']) and len(pos) >= 6
                and pos[5].startswith(('終止形', '連体形'))):
            continue
        if (hazu['surface'], hazu['lemma'], hazu['pos'][:2]) != ('はず', 'はず', ['名詞', '普通名詞']):
            continue
        if (ga['surface'], ga['lemma'], ga['pos'][:1]) != ('が', 'が', ['助詞']):
            continue
        end = adjective_inflection_end(tokens, i + 2, lemma='ない', pos='形容詞',
                                       base='ない', past_stem='なかっ', continuative_stem='なく',
                                       conditional_stem='なけれ', forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            yield hazu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=35, id='hazuganai', expression=HAZUGANAI_EXPRESSION,
                   reading='はずがない', meaning='cannot be; there is no way that', match=match_hazuganai)

RULES = (RULE,)
