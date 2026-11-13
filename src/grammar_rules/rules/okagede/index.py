"""Attribution of an outcome to a helpful cause."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OKAGEDE_EXPRESSION = '〜おかげで'


def match_okagede(tokens):
    for i in range(1, len(tokens) - 1):
        previous, okage, de = tokens[i - 1:i + 2]
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        demonstrative = (previous['surface'] in ('その', 'この', 'あの', 'どの')
                         and previous['pos'][:1] == ['連体詞'])
        if not (predicate or nominal or demonstrative) or not adjacent(previous, okage):
            continue
        if (okage['surface'] not in ('おかげ', 'お蔭', 'お陰')
                or okage['lemma'] not in ('おかげ', 'お蔭', 'お陰')
                or okage['pos'][:1] != ['名詞'] or not adjacent(okage, de)
                or de['surface'] != 'で' or de['lemma'] != 'で'
                or de['pos'][:1] != ['助詞']):
            continue
        yield okage['span'][0], de['span'][1]


RULE = GrammarRule(order=122, id='okagede', expression=OKAGEDE_EXPRESSION,
                   reading='おかげで', meaning='thanks to; owing to', match=match_okagede)
RULES = (RULE,)
