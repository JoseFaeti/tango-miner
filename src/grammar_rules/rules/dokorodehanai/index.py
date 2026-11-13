"""Source-aligned どころではない construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches, polite_suffix_end

DOKORODEHANAI_EXPRESSION = '〜どころではない'


def match_dokorodehanai(tokens):
    for i in range(1, len(tokens) - 2):
        previous, dokoro = tokens[i - 1:i + 1]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or dokoro['surface'] != 'どころ' or dokoro['lemma'] != 'どころ'
                or dokoro['pos'][:2] not in (['助詞', '副助詞'], ['名詞', '普通名詞'])
                or not adjacent(previous, dokoro) or not adjacent(dokoro, tokens[i + 1])):
            continue
        particle = tokens[i + 1]
        if particle['surface'] == 'じゃ' and particle['lemma'] == 'だ' and particle['pos'][:1] == ['助動詞']:
            end = i + 1
        elif (particle['surface'] == 'で'
              and ((particle['lemma'] == 'だ' and particle['pos'][:1] == ['助動詞'])
                   or (particle['lemma'] == 'で' and particle['pos'][:2] == ['助詞', '格助詞']))
              and next_matches(tokens, i + 1, 'は', '助詞', 'は')):
            end = i + 2
        else:
            continue
        if next_matches(tokens, end, 'ない', '形容詞'):
            end = negative_suffix_end(tokens, end + 1, pos='形容詞', continuative=True)
        elif (next_matches(tokens, end, 'ある', '動詞', 'あり')
              and next_matches(tokens, end + 1, 'ます', '助動詞')):
            end = polite_suffix_end(tokens, end + 2, forms=('negative',))
        else:
            continue
        if end is not None:
            yield dokoro['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='dokorodehanai', expression=DOKORODEHANAI_EXPRESSION,
                   reading='どころではない', meaning='not in a situation to; far from being able to',
                   match=match_dokorodehanai)
RULES = (RULE,)
