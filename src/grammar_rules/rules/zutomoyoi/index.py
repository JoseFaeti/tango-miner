"""Literary verbal no-obligation construction ずともよい."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜ずともよい'
_FORMS = frozenset(('ずともよい', 'ずとも良い', 'ずともいい',
                    'ずともよかった', 'ずとも良かった'))
_PREFIXES = frozenset(form[:i] for form in _FORMS for i in range(len(form) + 1))


def match_zutomoyoi(tokens):
    for i in range(1, len(tokens)):
        anchor, zu = tokens[i - 1:i + 1]
        pos = anchor['pos']
        if (len(pos) < 6 or not pos[5].startswith('未然形')
                or not (pos[:1] == ['動詞'] or
                        (pos[:1] == ['助動詞'] and anchor['lemma'] in
                         ('れる', 'られる', 'せる', 'させる')))
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not adjacent(anchor, zu)):
            continue
        surface = ''
        for j in range(i, min(len(tokens), i + 7)):
            if j > i and not adjacent(tokens[j - 1], tokens[j]):
                break
            surface += tokens[j]['surface']
            if surface in _FORMS:
                yield zu['span'][0], tokens[j]['span'][1]
                break
            if surface not in _PREFIXES:
                break


RULE = GrammarRule(id='zutomoyoi', expression=EXPRESSION, reading='ずともよい',
                   meaning='need not; do not have to', match=match_zutomoyoi, order=1)
RULES = (RULE,)
