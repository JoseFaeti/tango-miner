"""Single につけ meaning whenever or on the occasion of."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nitsukenitsuke.index import match_nitsukenitsuke


def match_nitsuke(tokens):
    paired = list(match_nitsukenitsuke(tokens))
    # A paired match identifies its first marker; find the remaining markers
    # within the same sentence so neither alternative becomes a single card.
    boundaries = {'。', '！', '？', '!', '?', '\n'}
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
        if end + 1 < len(tokens) and adjacent(tokens[end], tokens[end + 1]):
            following = tokens[end + 1]
            if following['pos'][:1] == ['助動詞'] or following['surface'] in ('て', 'ても', 'ては'):
                continue
        start_span, end_span = marker['span'][0], tokens[end]['span'][1]
        sentence_start = max((t['span'][1] for t in tokens[:i] if t['surface'] in boundaries), default=0)
        sentence_end = next((t['span'][0] for t in tokens[end + 1:] if t['surface'] in boundaries), float('inf'))
        if any(sentence_start <= a < sentence_end for a, _ in paired):
            continue
        yield start_span, end_span


RULE = GrammarRule(order=2, id='nitsuke', expression='〜につけ', reading='につけ',
                   meaning='whenever; on the occasion of', match=match_nitsuke)
RULES = (RULE,)
