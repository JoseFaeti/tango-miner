"""Noun + なしに / なしでは: without something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nashini_nashideha(tokens):
    for i in range(1, len(tokens) - 1):
        previous, nashi, particle = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (previous['pos'][:1] == ['動詞']
                    and (len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')))
                or not adjacent(previous, nashi)
                or nashi['surface'] not in ('なし', '無し')
                or nashi['pos'][:1] != ['名詞']
                or not adjacent(nashi, particle)
                or particle['pos'][:1] != ['助詞']):
            continue
        if particle['surface'] == 'に':
            yield nashi['span'][0], particle['span'][1]
        elif (particle['surface'] == 'で' and i + 2 < len(tokens)
              and tokens[i + 2]['surface'] == 'は'
              and tokens[i + 2]['pos'][:1] == ['助詞']
              and adjacent(particle, tokens[i + 2])):
            yield nashi['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(id='nashini_nashideha', expression='〜なしに / 〜なしでは',
                   reading='なしに / なしでは', meaning='without; without it (cannot)',
                   match=match_nashini_nashideha, order=1)
RULES = (RULE,)
