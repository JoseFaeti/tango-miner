"""Adnominal predicate followed by ところを."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOKOROO_EXPRESSION = '〜ところを'


def match_tokoroo_prefix(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, particle = tokens[i - 1:i + 2]
        if not (noun['surface'] in ('ところ', '所')
                and noun['lemma'] in ('ところ', '所')
                and noun['pos'][:1] == ['名詞']
                and particle['surface'] == particle['lemma'] == 'を'
                and particle['pos'][:1] == ['助詞']
                and adjacent(previous, noun) and adjacent(noun, particle)):
            continue
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞']) and len(pos) >= 6
                     and pos[5].startswith('連体形'))
        auxiliary = (pos[:1] == ['助動詞'] and len(pos) >= 6
                     and pos[5].startswith('連体形') and i >= 2
                     and tokens[i - 2]['pos'][:1] in (['動詞'], ['形容詞'], ['形状詞'], ['名詞'], ['助動詞'])
                     and adjacent(tokens[i - 2], previous))
        if predicate or auxiliary:
            yield noun['span'][0], particle['span'][1]


def match_tokoroo(tokens):
    prefixes = set(match_tokoroo_prefix(tokens))
    for i in range(len(tokens) - 2):
        noun, particle, following = tokens[i:i + 3]
        if ((noun['span'][0], particle['span'][1]) in prefixes
                and adjacent(particle, following)
                and ((following['pos'][:1] == ['動詞']
                      and following['lemma'] in ('助ける', '捕まえる', 'つかまえる',
                                                  '呼び止める', '起こす', '邪魔する'))
                     or following['lemma'] in ('ありがとう', '有難う')
                     or (following['lemma'] == '邪魔'
                         and i + 3 < len(tokens)
                         and tokens[i + 3]['lemma'] == 'する'
                         and adjacent(following, tokens[i + 3])))):
            yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(order=103, id='tokoroo', expression=TOKOROO_EXPRESSION,
                   reading='ところを', meaning='when; although; despite the situation',
                   match=match_tokoroo)
RULES = (RULE,)
