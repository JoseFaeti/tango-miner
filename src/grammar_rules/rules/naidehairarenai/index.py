"""Cannot help doing: verb + ないで(は)いられない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end
from src.grammar_rules.rules.nai.index import match_nai

NAIDEHAIRARENAI_EXPRESSION = '〜ないではいられない'


def match_naidehairarenai(tokens):
    starts = {start for start, end in match_nai(tokens)}
    for i in range(len(tokens) - 4):
        negative, de = tokens[i:i + 2]
        if not (negative['span'][0] in starts and negative['surface'] == 'ない'
                and de['surface'] == 'で' and de['pos'][:1] in (['助詞'], ['助動詞'])
                and adjacent(negative, de)):
            continue
        j = i + 2
        if tokens[j]['surface'] == 'は' and tokens[j]['pos'][:1] == ['助詞']:
            if not adjacent(de, tokens[j]):
                continue
            j += 1
        if j + 2 >= len(tokens):
            continue
        iru, rare, suffix = tokens[j:j + 3]
        if not (adjacent(tokens[j - 1], iru) and adjacent(iru, rare)
                and adjacent(rare, suffix)
                and iru['surface'] in ('い', '居') and iru['lemma'] in ('いる', '居る')
                and iru['pos'][:1] == ['動詞']
                and rare['surface'] == 'られ' and rare['lemma'] == 'られる'
                and rare['pos'][:1] == ['助動詞']):
            continue
        end = negative_suffix_end(tokens, j + 2, pos='助動詞', continuative=True, conditional=True)
        if end is None and suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, j + 2, forms=('negative',))
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naidehairarenai', expression=NAIDEHAIRARENAI_EXPRESSION,
                   reading='ないではいられない', meaning='cannot help doing',
                   match=match_naidehairarenai, order=1)
RULES = (RULE,)
