"""Adnominal predicate followed by ところに or ところへ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOKORONI_TOKOROHE_EXPRESSION = '〜ところに / 〜ところへ'


def match_tokoroni_tokorohe(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, particle = tokens[i - 1:i + 2]
        if not (noun['surface'] in ('ところ', '所')
                and noun['lemma'] in ('ところ', '所')
                and noun['pos'][:1] == ['名詞']
                and particle['surface'] in ('に', 'へ')
                and particle['lemma'] == particle['surface']
                and particle['pos'][:1] == ['助詞']
                and adjacent(previous, noun) and adjacent(noun, particle)):
            continue
        pos = previous['pos']
        predicate = (pos[:1] == ['動詞'] and len(pos) >= 6
                     and pos[5].startswith('連体形'))
        auxiliary = (pos[:1] == ['助動詞'] and len(pos) >= 6
                     and pos[5].startswith('連体形'))
        if auxiliary:
            j = i - 1
            while j > 0 and tokens[j]['pos'][:1] == ['助動詞'] and adjacent(tokens[j - 1], tokens[j]):
                j -= 1
            auxiliary = tokens[j]['pos'][:1] == ['動詞']
        if predicate or auxiliary:
            yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=104, id='tokoroni_tokorohe',
                   expression=TOKORONI_TOKOROHE_EXPRESSION,
                   reading='ところに / ところへ', meaning='just when; at the moment when',
                   match=match_tokoroni_tokorohe)
RULES = (RULE,)
