"""Selected complete verbal no-obligation constructions."""
from src.GrammarConstants import NAKUTEMOII_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

_FORMS = frozenset(prefix + ending for prefix in ('なくても', 'なくったって') for ending in
                   ('いい', 'よい', '良い', 'よかった', '良かった'))
_PREFIXES = frozenset(form[:i] for form in _FORMS for i in range(len(form) + 1))


def match_nakutemoii(tokens):
    for i in range(1, len(tokens)):
        first_surface = tokens[i]['surface']
        if first_surface and first_surface not in _PREFIXES:
            continue
        anchor = tokens[i - 1]
        pos = anchor['pos']
        if (len(pos) < 6 or not pos[5].startswith('未然形')
                or not (pos[:1] == ['動詞'] or
                        (pos[:1] == ['助動詞'] and anchor['lemma'] in
                         ('れる', 'られる', 'せる', 'させる')))
                or not adjacent(anchor, tokens[i])):
            continue
        surface = ''
        for j in range(i, min(len(tokens), i + 8)):
            if j > i and not adjacent(tokens[j - 1], tokens[j]):
                break
            surface += tokens[j]['surface']
            if surface in _FORMS:
                yield tokens[i]['span'][0], tokens[j]['span'][1]
                break
            if surface not in _PREFIXES:
                break


RULE = GrammarRule(
    id='nakutemoii', expression=NAKUTEMOII_GRAMMAR_EXPRESSION,
    reading='なくてもいい', meaning='do not have to; need not',
    match=match_nakutemoii,
)
