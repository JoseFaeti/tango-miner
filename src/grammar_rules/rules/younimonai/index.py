"""Volitional action frustrated by an inability to perform it."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

YOUNIMONAI_EXPRESSION = '〜ようにも〜ない'


def match_younimonai(tokens):
    for i, first in enumerate(tokens):
        pos = first['pos']
        if ((pos[:1] == ['動詞'] and len(pos) >= 6 and pos[5].startswith('意志推量形'))
                or (first['surface'] == 'しよう' and pos[:1] == ['名詞'])):
            end = i
        elif (first['surface'] == '来' and pos[:1] == ['接頭辞']
              and i + 1 < len(tokens) and tokens[i + 1]['surface'] == 'よう'
              and adjacent(first, tokens[i + 1])):
            end = i + 1
        else:
            continue
        if end + 4 >= len(tokens):
            continue
        ni, mo, verb = tokens[end + 1:end + 4]
        if (ni['surface'] != 'に' or ni['pos'][:1] not in (['助詞'], ['助動詞'])
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(end, end + 3))):
            continue
        j = end + 4
        if not adjacent(verb, tokens[j]):
            continue
        if tokens[j]['lemma'] in ('られる', 'れる') and tokens[j]['pos'][:1] == ['助動詞']:
            j += 1
            if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
                continue
        last = negative_suffix_end(tokens, j, pos='助動詞')
        if last is None:
            last = polite_suffix_end(tokens, j, forms=('negative',))
        if last is not None:
            yield first['span'][0], tokens[last]['span'][1]


RULE = GrammarRule(order=2, id='younimonai', expression=YOUNIMONAI_EXPRESSION,
                   reading='ようにもない', meaning='cannot do even if one tries',
                   match=match_younimonai)
RULES = (RULE,)
