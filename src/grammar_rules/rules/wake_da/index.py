'''Predicate-attached explanatory and partial-negative わけ.'''
from src.GrammarConstants import WAKE_DA, WAKE_NEGATIVE
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_predicate_end


def matches(tokens, negative):
    for i in range(1, len(tokens) - 1):
        prev, wake = tokens[i - 1:i + 1]
        pos = prev['pos']
        if not (adjacent(prev, wake) and wake['surface'] in ('わけ', '訳')
                and wake['pos'][:2] == ['名詞', '普通名詞']
                and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))
                and adjacent(wake, tokens[i + 1])):
            continue
        suffix = tokens[i + 1]
        if not negative:
            if suffix['pos'][:1] != ['助動詞']:
                continue
            if (suffix['lemma'], suffix['surface']) in (('だ', 'だ'), ('です', 'です')):
                end = i + 1
            elif ((suffix['lemma'], suffix['surface']) in (('だ', 'だっ'), ('です', 'でし'))
                  and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
                end = i + 2
            else:
                continue
        else:
            if (suffix['surface'] == 'じゃ' and suffix['lemma'] == 'だ'
                    and suffix['pos'][:1] == ['助動詞']):
                start = i + 2
            elif (suffix['surface'] == 'で' and suffix['pos'][:1] == ['助詞']
                  and next_matches(tokens, i + 1, 'は', '助詞', 'は')):
                start = i + 3
            else:
                continue
            if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
                continue
            end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
            if end is None:
                continue
            if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
        yield wake['span'][0], tokens[end]['span'][1]


def match_explanation(tokens):
    yield from matches(tokens, False)


def match_negative(tokens):
    yield from matches(tokens, True)


EXPLANATION = GrammarRule(id='wake_da', expression=WAKE_DA, reading='わけだ',
                          meaning='that is why; it follows that', match=match_explanation)
NEGATIVE = GrammarRule(id='wake_dewa_nai', expression=WAKE_NEGATIVE, reading='わけではない',
                       meaning='it does not mean that; not necessarily', match=match_negative)
