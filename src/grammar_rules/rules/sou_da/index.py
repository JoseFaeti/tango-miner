"""Conservative source-aligned appearance and hearsay そうだ forms."""
from src.GrammarConstants import SOU_APPEARANCE, SOU_HEARSAY
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def _match(tokens, appearance):
    for i in range(1, len(tokens) - 1):
        previous, sou, copula = tokens[i - 1:i + 2]
        if sou['surface'] != 'そう':
            continue
        if not adjacent(previous, sou) or not adjacent(sou, copula):
            continue
        expected = ['形状詞', '助動詞語幹'] if appearance else ['名詞', '助動詞語幹']
        if sou['surface'] != 'そう' or sou['lemma'] != 'そう' or sou['pos'][:2] != expected:
            continue
        pos = previous['pos']
        if appearance:
            attached = (pos[:1] == ['動詞'] and len(pos) >= 6 and pos[5].startswith('連用形'))
            attached |= (pos[:1] == ['形容詞'] and len(pos) >= 6 and pos[5].startswith('語幹'))
            attached |= (pos[:1] == ['形状詞'] or (len(pos) >= 3 and pos[0] == '名詞' and pos[2] == '形状詞可能'))
        else:
            attached = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞']) and len(pos) >= 6
                        and pos[5].startswith(('終止形', '連体形')))
        if not attached or copula['pos'][:1] != ['助動詞']:
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
    return _match(tokens, True)


def match_hearsay(tokens):
    return _match(tokens, False)


APPEARANCE = GrammarRule(id='sou_da_appearance', expression=SOU_APPEARANCE,
                         reading='そうだ', meaning='looks as if; seems likely', match=match_appearance)
HEARSAY = GrammarRule(id='sou_da_hearsay', expression=SOU_HEARSAY,
                      reading='そうだ', meaning='it is said that; reportedly', match=match_hearsay)
