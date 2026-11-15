"""Attached temporal 間 and 間に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

AIDA_AIDANI_EXPRESSION = '〜間 / 〜間に'


def match_aida_aidani(tokens):
    for i in range(1, len(tokens)):
        previous, noun = tokens[i - 1:i + 1]
        if (noun['surface'] not in ('間', 'あいだ')
                or noun['pos'][:3] != ['名詞', '普通名詞', '副詞可能']
                or not adjacent(previous, noun)):
            continue
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        if not (nominal or predicate):
            continue
        end = i
        if i + 1 < len(tokens) and adjacent(noun, tokens[i + 1]):
            following = tokens[i + 1]
            if (following['surface'] == 'に' and following['lemma'] == 'に'
                    and following['pos'][:2] == ['助詞', '格助詞']):
                end += 1
            elif following['pos'][:1] == ['助詞'] and following['surface'] not in ('は', 'も'):
                continue
        yield noun['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=105, id='aida_aidani', expression=AIDA_AIDANI_EXPRESSION,
                   reading='あいだ / あいだに', meaning='while; during; within',
                   match=match_aida_aidani)

RULES = (RULE,)
