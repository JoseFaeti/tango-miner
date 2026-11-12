"""Strong inference expressed by に違いない."""
from src.GrammarConstants import NICHIGAINAI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end


def match_nichigainai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, chigai, nai = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in ((previous, ni), (ni, chigai), (chigai, nai))):
            continue
        pos = previous['pos']
        nominal = pos[:1] in (['名詞'], ['代名詞'], ['形状詞'])
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞']) and len(pos) >= 6
                     and pos[5].startswith(('終止形', '連体形')))
        if not (nominal or predicate):
            continue
        if ni['surface'] != 'に' or (ni['lemma'], ni['pos'][0]) not in (('に', '助詞'), ('だ', '助動詞')):
            continue
        if (chigai['surface'] not in ('違い', 'ちがい') or chigai['lemma'] != chigai['surface']
                or chigai['pos'][:2] != ['名詞', '普通名詞']):
            continue
        end = adjective_inflection_end(tokens, i + 2, lemma='ない', pos='形容詞',
                                       base='ない', past_stem='なかっ', continuative_stem='なく',
                                       conditional_stem='なけれ', forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nichigainai', expression=NICHIGAINAI_EXPRESSION,
                   reading='にちがいない', meaning='must be; surely', match=match_nichigainai)
