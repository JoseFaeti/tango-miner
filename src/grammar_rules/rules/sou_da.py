"""Selected appearance and hearsay analyses of そうだ."""
from src.GrammarConstants import SOU_DA_APPEARANCE, SOU_DA_HEARSAY
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def _matches(tokens, appearance):
    for i in range(1, len(tokens) - 1):
        prev, sou, copula = tokens[i - 1:i + 2]
        if not adjacent(prev, sou) or not adjacent(sou, copula):
            continue
        if sou['surface'] != 'そう' or sou['lemma'] != 'そう':
            continue
        if sou['pos'][:2] != (['形状詞', '助動詞語幹'] if appearance else ['名詞', '助動詞語幹']):
            continue
        pos = prev['pos']
        if appearance:
            supported = (len(pos) >= 6 and (
                (pos[0] == '動詞' and pos[5].startswith('連用形')) or
                (pos[0] == '形容詞' and pos[5].startswith('語幹')) or
                (pos[0] == '形状詞') or
                (pos[0] == '名詞' and pos[2] == '形状詞可能')))
        else:
            supported = (len(pos) >= 6 and pos[0] in ('動詞', '形容詞', '助動詞')
                         and pos[5].startswith(('終止形', '連体形')))
        if not supported or copula['pos'][:1] != ['助動詞']:
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif (appearance and (copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield sou['span'][0], tokens[end]['span'][1]


def match_appearance(tokens):
    yield from _matches(tokens, True)


def match_hearsay(tokens):
    yield from _matches(tokens, False)


APPEARANCE = GrammarRule(id='sou_da_appearance', expression=SOU_DA_APPEARANCE,
                         reading='そうだ', meaning='looks as if; seems likely to', match=match_appearance)
HEARSAY = GrammarRule(id='sou_da_hearsay', expression=SOU_DA_HEARSAY,
                      reading='そうだ', meaning='I hear that; reportedly', match=match_hearsay)
