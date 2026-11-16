"""Inability to suppress a feeling: noun + を禁じ得ない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

OKINJIENAI_EXPRESSION = '〜を禁じ得ない'


def match_okinjienai(tokens):
    for i in range(1, len(tokens) - 3):
        noun, wo, kinji, eru = tokens[i - 1:i + 3]
        if not (noun['pos'][:1] == ['名詞']
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and kinji['surface'] == '禁じ' and kinji['lemma'] in ('禁ずる', '禁じる')
                and kinji['pos'][:1] == ['動詞']
                and eru['surface'] in ('得', 'え') and eru['lemma'] in ('得る', 'える')
                and eru['pos'][:1] == ['動詞']
                and all(adjacent(a, b) for a, b in zip((noun, wo, kinji), (wo, kinji, eru)))
                and adjacent(eru, tokens[i + 3])):
            continue
        suffix = tokens[i + 3]
        end = negative_suffix_end(tokens, i + 3, pos='助動詞', continuative=True, conditional=True)
        if end is None and suffix['surface'] in ('ず', 'ぬ') and suffix['pos'][:1] == ['助動詞']:
            end = i + 3
        if end is None and suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield wo['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='okinjienai', expression=OKINJIENAI_EXPRESSION,
                   reading='をきんじえない', meaning='cannot suppress a feeling',
                   match=match_okinjienai, order=1)
RULES = (RULE,)
