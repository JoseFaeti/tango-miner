"""Inability to act because of circumstances or social obligations."""
WAKENIHAIKANAI_EXPRESSION = '〜わけにはいかない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


_FORMS = frozenset(wake + particle + ending
                   for wake in ('わけ', '訳') for particle in ('には', 'に')
                   for ending in ('いかない', 'いかなかった', 'いきません', 'いきませんでした',
                                  '行かない', '行かなかった', '行きません', '行きませんでした'))
_PREFIXES = frozenset(form[:i] for form in _FORMS for i in range(1, len(form) + 1))


def match_wakenihaikanai(tokens):
    for i in range(1, len(tokens)):
        anchor, wake = tokens[i - 1:i + 1]
        pos = anchor['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith(('終止形', '連体形'))
                or not adjacent(anchor, wake)
                or wake['surface'] not in ('わけ', '訳')
                or wake['lemma'] not in ('わけ', '訳')
                or wake['pos'][:2] != ['名詞', '普通名詞']):
            continue
        yield from match_ending(tokens, i)


def match_ending(tokens, i):
    wake = tokens[i]
    surface = ''
    for j in range(i, min(len(tokens), i + 10)):
        if j > i and not adjacent(tokens[j - 1], tokens[j]):
            break
        surface += tokens[j]['surface']
        if surface in _FORMS:
            if j + 1 < len(tokens) and adjacent(tokens[j], tokens[j + 1]):
                following = tokens[j + 1]
                if following['surface'] in ('た', 'でし') and following['pos'][:1] == ['助動詞']:
                    continue
            yield wake['span'][0], tokens[j]['span'][1]
            break
        if surface not in _PREFIXES:
            break


RULE = GrammarRule(order=33, id='wakenihaikanai', expression=WAKENIHAIKANAI_EXPRESSION,
                   reading='わけにはいかない', meaning='cannot reasonably do; cannot bring oneself to do',
                   match=match_wakenihaikanai)

RULES = (RULE,)
