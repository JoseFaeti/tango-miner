"""Cannot help doing: verb + ずに(は)いられない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

ZUNIHAIRARENAI_EXPRESSION = '〜ずにはいられない'


def match_zunihairarenai(tokens):
    for i in range(1, len(tokens) - 4):
        verb, zu, ni = tokens[i - 1:i + 2]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and adjacent(verb, zu) and adjacent(zu, ni)):
            continue
        j = i + 2
        if tokens[j]['surface'] == 'は' and tokens[j]['pos'][:1] == ['助詞']:
            if not adjacent(ni, tokens[j]):
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
            yield zu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='zunihairarenai', expression=ZUNIHAIRARENAI_EXPRESSION,
                   reading='ずにはいられない', meaning='cannot help doing',
                   match=match_zunihairarenai, order=1)
RULES = (RULE,)
