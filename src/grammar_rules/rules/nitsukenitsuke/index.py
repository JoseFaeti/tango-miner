"""Paired につけ alternatives."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitsukenitsuke(tokens):
    markers = []
    for i in range(1, len(tokens)):
        before, marker = tokens[i - 1:i + 1]
        if before['pos'][:1] not in (['名詞'], ['代名詞'], ['形容詞'], ['動詞']):
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
        if end + 1 < len(tokens) and tokens[end + 1]['pos'][:1] == ['助動詞'] and adjacent(tokens[end], tokens[end + 1]):
            continue
        markers.append((i, end))
    for (first, end), (second, _) in zip(markers, markers[1:]):
        between = tokens[end + 1:second]
        if not between or any(t['surface'] in ('。', '！', '？', '!', '?', '\n') for t in between):
            continue
        yield tokens[first]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='nitsukenitsuke', expression='〜につけ〜につけ',
                   reading='につけにつけ', meaning='whether ... or ...; both ... and ...',
                   match=match_nitsukenitsuke)
RULES = (RULE,)
