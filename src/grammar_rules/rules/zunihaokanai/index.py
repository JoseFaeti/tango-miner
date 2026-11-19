"""Verb + ずにはおかない: inevitably cause or insist on doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_zunihaokanai(tokens):
    for i in range(1, len(tokens) - 4):
        verb, zu, ni, wa, oku, suffix = tokens[i - 1:i + 5]
        if not (verb['pos'][:1] in (['動詞'], ['助動詞']) and len(verb['pos']) >= 6
                and (verb['pos'][:1] == ['動詞'] or verb['lemma'] in ('せる', 'させる', 'れる', 'られる'))
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and wa['surface'] == 'は' and wa['pos'][:1] == ['助詞']
                and oku['lemma'] in ('置く', 'おく') and oku['pos'][:1] == ['動詞']
                and oku['surface'] in ('おか', '置か', 'おき', '置き')
                and all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        if oku['surface'] in ('おか', '置か'):
            end = negative_suffix_end(tokens, i + 4, pos='助動詞', continuative=True, conditional=True)
        else:
            end = polite_suffix_end(tokens, i + 4, forms=('negative',))
        if end is not None:
            yield zu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='zunihaokanai', expression='〜ずにはおかない',
                   reading='ずにはおかない',
                   meaning='inevitably cause; will not leave without doing',
                   match=match_zunihaokanai, order=1)
RULES = (RULE,)
