"""Sentence-final elliptical verbal obligations."""
from src.GrammarConstants import ELLIPTICAL_OBLIGATION_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


_FORMS = frozenset(('ないと', 'なくちゃ', 'なきゃ'))
_PREFIXES = frozenset(form[:i] for form in _FORMS for i in range(1, len(form) + 1))
_FINAL_PARTICLES = frozenset(('ね', 'な', 'よ', 'わ', 'ぞ', 'さ'))
_END_MARKS = frozenset(('。', '！', '？', '!', '?', '…', '」', '』'))


def _at_utterance_end(tokens, index):
    """Accept only a final particle sequence followed by an utterance boundary."""
    previous = tokens[index]
    for token in tokens[index + 1:]:
        if not adjacent(previous, token):
            return False
        if token['surface'] in _END_MARKS:
            return True
        if token['surface'] not in _FINAL_PARTICLES or token['pos'][:1] != ['助詞']:
            return False
        previous = token
    return True


def match_elliptical_obligation(tokens):
    for i in range(1, len(tokens)):
        first_surface = tokens[i]['surface']
        if first_surface and first_surface not in _PREFIXES:
            continue
        anchor = tokens[i - 1]
        pos = anchor['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith(('未然形', '連用形'))
                or not adjacent(anchor, tokens[i])):
            continue
        surface = ''
        for j in range(i, min(len(tokens), i + 4)):
            if j > i and not adjacent(tokens[j - 1], tokens[j]):
                break
            surface += tokens[j]['surface']
            if surface in _FORMS and _at_utterance_end(tokens, j):
                yield tokens[i]['span'][0], tokens[j]['span'][1]
                break
            if surface not in _PREFIXES:
                break


RULE = GrammarRule(
    id='elliptical_obligation', expression=ELLIPTICAL_OBLIGATION_EXPRESSION,
    reading='ないと / なくちゃ / なきゃ',
    meaning='must; have to; need to (elliptical)', match=match_elliptical_obligation,
)
