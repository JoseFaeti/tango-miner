"""Verb te-form followed by 済む (suffice)."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, has_next, negative_suffix_end, next_matches, polite_suffix_end

TESUMU_EXPRESSION = '〜てすむ'


def match_tesumu(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, sumu = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or verb['lemma'] == 'ない' or len(verb['pos']) < 6
                or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, connective) or not adjacent(connective, sumu)
                or sumu['lemma'] not in ('済む', 'すむ') or sumu['pos'][:1] != ['動詞']):
            continue
        end = sumu_end(tokens, i + 1)
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


def sumu_end(tokens, end):
    surface = tokens[end]['surface']
    if surface in ('済む', 'すむ'):
        pass
    elif surface in ('済ん', 'すん') and (next_matches(tokens, end, 'だ', '助動詞', 'だ')
                                            or next_matches(tokens, end, 'て', '助詞', 'で')):
        end += 1
    elif surface in ('済み', 'すみ') and next_matches(tokens, end, 'ます', '助動詞'):
        end = polite_suffix_end(tokens, end + 1)
    elif surface in ('済め', 'すめ') and next_matches(tokens, end, 'ば', '助詞', 'ば'):
        end += 1
    elif (surface in ('済ま', 'すま') and has_next(tokens, end)
          and tokens[end + 1]['lemma'] == 'ない'):
        end = negative_suffix_end(tokens, end + 1, pos='助動詞', continuative=True, extended=True)
    else:
        return None
    return end


RULE = GrammarRule(id='tesumu', expression=TESUMU_EXPRESSION, reading='てすむ',
                   meaning='suffice to do; get away with doing', match=match_tesumu, order=145)
RULES = (RULE,)
