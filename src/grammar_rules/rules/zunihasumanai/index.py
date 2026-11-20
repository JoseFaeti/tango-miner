"""Verb + ずには済まない: cannot get away without doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_zunihasumanai(tokens):
    for i in range(1, len(tokens) - 4):
        verb, zu, ni, wa, sumu, suffix = tokens[i - 1:i + 5]
        if not (verb['pos'][:1] in (['動詞'], ['助動詞']) and len(verb['pos']) >= 6
                and (verb['pos'][:1] == ['動詞'] or verb['lemma'] in ('せる', 'させる', 'れる', 'られる'))
                and verb['pos'][5].startswith('未然形')
                and zu['surface'] == 'ず' and zu['pos'][:1] == ['助動詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and wa['surface'] == 'は' and wa['pos'][:1] == ['助詞']
                and sumu['lemma'] in ('済む', 'すむ') and sumu['pos'][:1] == ['動詞']
                and sumu['surface'] in ('済ま', 'すま', '済み', 'すみ')
                and all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            continue
        if sumu['surface'] in ('済ま', 'すま'):
            end = negative_suffix_end(tokens, i + 4, pos='助動詞', continuative=True, conditional=True)
        else:
            end = polite_suffix_end(tokens, i + 4, forms=('negative',))
        if end is not None:
            yield zu['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='zunihasumanai', expression='〜ずにはすまない',
                   reading='ずにはすまない', meaning='cannot get away without doing',
                   match=match_zunihasumanai, order=1)
RULES = (RULE,)
