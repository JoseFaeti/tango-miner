"""Concessive につけても: whatever the occasion."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitsuketemo(tokens):
    for i in range(1, len(tokens)):
        before, marker = tokens[i - 1:i + 1]
        if before['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞']):
            continue
        if not adjacent(before, marker):
            continue
        end = i
        if marker['surface'] == 'につけ' and marker['lemma'] == 'につけ':
            pass
        elif (marker['surface'] == 'に' and marker['pos'][:2] == ['助詞', '格助詞']
              and i + 1 < len(tokens) and adjacent(marker, tokens[i + 1])
              and tokens[i + 1]['surface'] == 'つけ'
              and tokens[i + 1]['lemma'] == 'つける'
              and tokens[i + 1]['pos'][:1] == ['動詞']):
            end += 1
        else:
            continue
        if end + 1 >= len(tokens) or not adjacent(tokens[end], tokens[end + 1]):
            continue
        following = tokens[end + 1]
        if following['surface'] == 'ても' and following['pos'][:1] == ['助詞']:
            end += 1
        elif (following['surface'] == 'て' and following['pos'][:1] == ['助詞']
              and end + 2 < len(tokens) and adjacent(following, tokens[end + 2])
              and tokens[end + 2]['surface'] == 'も'
              and tokens[end + 2]['pos'][:1] == ['助詞']):
            end += 2
        else:
            continue
        yield marker['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='nitsuketemo', expression='〜につけても', reading='につけても',
                   meaning='whatever the occasion; even in relation to', match=match_nitsuketemo)
RULES = (RULE,)
