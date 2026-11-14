"""Verb + ないではおかない: inevitably cause or insist on doing."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end
from src.grammar_rules.rules.nai.index import match_nai


def match_naidehaokanai(tokens):
    starts = {start for start, _ in match_nai(tokens)}
    for i in range(len(tokens) - 4):
        negative, de, wa, oku, suffix = tokens[i:i + 5]
        if not (negative['span'][0] in starts and negative['surface'] == 'ない'
                and de['surface'] == 'で' and de['pos'][:1] in (['助詞'], ['助動詞'])
                and wa['surface'] == 'は' and wa['pos'][:1] == ['助詞']
                and oku['lemma'] in ('置く', 'おく') and oku['pos'][:1] == ['動詞']
                and oku['surface'] in ('おか', '置か', 'おき', '置き')
                and all(adjacent(a, b) for a, b in zip(tokens[i:i + 4], tokens[i + 1:i + 5]))):
            continue
        if oku['surface'] in ('おか', '置か'):
            end = negative_suffix_end(tokens, i + 4, pos='助動詞', continuative=True, conditional=True)
        else:
            end = polite_suffix_end(tokens, i + 4, forms=('negative',))
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naidehaokanai', expression='〜ないではおかない',
                   reading='ないではおかない',
                   meaning='inevitably cause; will not leave without doing',
                   match=match_naidehaokanai, order=1)
RULES = (RULE,)
