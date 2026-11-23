"""Attributive predicate + ところによると, a reported source."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.niyoruto_niyoreba.index import match_niyoruto_niyoreba

EXPRESSION = '〜ところによると'


def match_tokoroniyoruto(tokens):
    attribution = set(match_niyoruto_niyoreba(tokens))
    for i in range(1, len(tokens) - 3):
        previous, noun, ni, _verb, ending = tokens[i - 1:i + 4]
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞']) and len(pos) >= 6
                     and pos[5].startswith('連体形'))
        auxiliary = (pos[:1] == ['助動詞'] and len(pos) >= 6
                     and pos[5].startswith('連体形') and i >= 2
                     and tokens[i - 2]['pos'][:1] in
                     (['動詞'], ['形容詞'], ['形状詞'], ['名詞'], ['助動詞'])
                     and adjacent(tokens[i - 2], previous))
        if (noun['surface'] in ('ところ', '所')
                and noun['lemma'] in ('ところ', '所')
                and noun['pos'][:1] == ['名詞']
                and (predicate or auxiliary) and adjacent(previous, noun)
                and ending['surface'] == 'と'
                and (ni['span'][0], ending['span'][1]) in attribution):
            yield noun['span'][0], ending['span'][1]


RULE = GrammarRule(order=103, id='tokoroniyoruto', expression=EXPRESSION,
                   reading='ところによると', meaning='according to (what one heard, read, etc.)',
                   match=match_tokoroniyoruto)
RULES = (RULE,)
