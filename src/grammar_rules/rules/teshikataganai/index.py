"""An uncontrollable feeling expressed with て仕方がない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import (adjacent, adjective_inflection_end,
                                       negative_suffix_end, next_matches, polite_suffix_end)

TESHIKATAGANAI_EXPRESSION = '〜てしかたがない / 〜てしょうがない'


def match_teshikataganai(tokens):
    for i in range(1, len(tokens) - 1):
        previous, connective, word = tokens[i - 1:i + 2]
        if not adjacent(previous, connective) or not adjacent(connective, word):
            continue
        if connective['surface'] == 'て':
            valid = (connective['pos'][:2] == ['助詞', '接続助詞']
                     and previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形'))
        elif connective['surface'] == 'で':
            valid = (connective['pos'][:2] == ['助詞', '接続助詞']
                     and previous['pos'][:1] in (['動詞'], ['助動詞'])
                     and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')
                     or connective['pos'][:1] in (['助詞'], ['助動詞'])
                     and previous['pos'][:1] in (['名詞'], ['形状詞']))
        else:
            continue
        if not valid:
            continue
        end = None
        if word['lemma'] == 'しかたない' and word['pos'][:1] == ['形容詞']:
            end = adjective_inflection_end(tokens, i + 1, lemma='しかたない', pos='形容詞',
                                           base='しかたない', past_stem='しかたなかっ',
                                           continuative_stem='しかたなく', conditional_stem='しかたなけれ',
                                           forms=('base', 'past', 'te', 'conditional'))
        elif word['surface'] in ('仕方', 'しかた', 'しょう', 'しよう', '仕様') and word['pos'][:1] == ['名詞']:
            j = i + 1
            if next_matches(tokens, j, 'が', '助詞', 'が'):
                j += 1
            if next_matches(tokens, j, 'ない', '形容詞'):
                end = negative_suffix_end(tokens, j + 1, pos='形容詞', conditional=True)
                if (tokens[j + 1]['surface'] == 'なく'
                        and next_matches(tokens, j + 1, 'て', '助詞', 'て')):
                    end = j + 2
            elif next_matches(tokens, j, 'ある', '動詞', 'あり') and next_matches(tokens, j + 1, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, j + 2, forms=('negative',))
        if end is not None:
            if next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=36, id='teshikataganai', expression=TESHIKATAGANAI_EXPRESSION,
                   reading='てしかたがない / てしょうがない',
                   meaning='cannot help feeling; unbearably', match=match_teshikataganai)
RULES = (RULE,)
