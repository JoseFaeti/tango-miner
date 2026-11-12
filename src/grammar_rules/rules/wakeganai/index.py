"""Predicate-attached inference that something cannot be the case."""
from src.GrammarConstants import WAKEGANAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end


def match_wakeganai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, wake, ga, nai = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in ((previous, wake), (wake, ga), (ga, nai))):
            continue
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞']) and len(pos) >= 6
                and pos[5].startswith(('終止形', '連体形'))):
            continue
        if wake['surface'] not in ('わけ', '訳') or wake['lemma'] not in ('わけ', '訳') or wake['pos'][:2] != ['名詞', '普通名詞']:
            continue
        if (ga['surface'], ga['lemma'], ga['pos'][:1]) != ('が', 'が', ['助詞']):
            continue
        end = adjective_inflection_end(tokens, i + 2, lemma='ない', pos='形容詞',
                                       base='ない', past_stem='なかっ', continuative_stem='なく',
                                       conditional_stem='なけれ', forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            yield wake['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='wakeganai', expression=WAKEGANAI_EXPRESSION,
                   reading='わけがない', meaning='cannot be; there is no way that', match=match_wakeganai)
