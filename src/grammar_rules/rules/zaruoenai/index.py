"""Unavoidable action expressed by verb + ざるを得ない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

ZARUOENAI_EXPRESSION = '〜ざるを得ない'


def match_zaruoenai(tokens):
    for i in range(1, len(tokens) - 3):
        verb, zaru, wo, eru = tokens[i - 1:i + 3]
        if not (verb['pos'][:1] == ['動詞']
                and zaru['surface'] == 'ざる' and zaru['pos'][:1] == ['助動詞']
                and wo['surface'] == 'を' and wo['pos'][:1] == ['助詞']
                and eru['surface'] in ('得', 'え') and eru['lemma'] in ('得る', 'える')
                and eru['pos'][:1] == ['動詞']
                and all(adjacent(a, b) for a, b in zip((verb, zaru, wo), (zaru, wo, eru)))
                and adjacent(eru, tokens[i + 3])):
            continue
        suffix = tokens[i + 3]
        end = negative_suffix_end(tokens, i + 3, pos='助動詞', continuative=True, conditional=True)
        if end is None and suffix['surface'] in ('ず', 'ぬ') and suffix['pos'][:1] == ['助動詞']:
            end = i + 3
        if end is None and suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield zaru['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='zaruoenai', expression=ZARUOENAI_EXPRESSION,
                   reading='ざるをえない', meaning='cannot avoid doing; have no choice but to',
                   match=match_zaruoenai, order=1)
RULES = (RULE,)
