"""Attribution of an undesirable outcome to a cause."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SEIDE_EXPRESSION = '〜せいで'


def match_seide(tokens):
    for i in range(1, len(tokens) - 1):
        previous, sei, de = tokens[i - 1:i + 2]
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        demonstrative = (previous['surface'] in ('その', 'この', 'あの', 'どの')
                         and previous['pos'][:1] == ['連体詞'])
        if not (predicate or nominal or demonstrative) or not adjacent(previous, sei):
            continue
        if (sei['surface'] not in ('せい', '所為') or sei['lemma'] not in ('せい', '所為')
                or sei['pos'][:1] != ['名詞'] or not adjacent(sei, de)
                or de['surface'] != 'で' or de['lemma'] != 'で'
                or de['pos'][:1] != ['助詞']):
            continue
        yield sei['span'][0], de['span'][1]


RULE = GrammarRule(order=122, id='seide', expression=SEIDE_EXPRESSION,
                   reading='せいで', meaning='because of; due to (undesirable cause)', match=match_seide)
RULES = (RULE,)
