"""Selected verb prohibition constructions."""
TE_WA_IKENAI_GRAMMAR_EXPRESSION = '〜てはいけない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


_FORMS = frozenset(prefix + ending for prefix in ('ては', 'では', 'ちゃ', 'じゃ')
                   for ending in ('いけない', 'いけなかった', 'いけません', 'いけませんでした'))
_NARANAI_FORMS = frozenset(prefix + ending for prefix in ('ては', 'では', 'ちゃ', 'じゃ')
                           for ending in ('ならない', 'ならなかった', 'なりません',
                                          'なりませんでした', 'ならぬ', 'ならん'))
_PREFIXES = {forms: frozenset(form[:i] for form in forms for i in range(len(form) + 1))
             for forms in (_FORMS, _NARANAI_FORMS)}


def match_te_wa_ikenai(tokens):
    yield from _match_prohibition(tokens, _FORMS)


def match_te_wa_naranai(tokens):
    yield from _match_prohibition(tokens, _NARANAI_FORMS)


def _match_prohibition(tokens, forms):
    prefixes = _PREFIXES[forms]
    for i in range(1, len(tokens)):
        anchor, first = tokens[i - 1], tokens[i]
        if first['surface'] not in prefixes:
            continue
        pos = anchor['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6 or not pos[5].startswith('連用形')
                or not adjacent(anchor, first) or first['pos'][:2] != ['助詞', '接続助詞']):
            continue
        surface = ''
        for j in range(i, min(len(tokens), i + 9)):
            if j > i and not adjacent(tokens[j - 1], tokens[j]):
                break
            surface += tokens[j]['surface']
            if surface in forms:
                if j + 1 < len(tokens) and adjacent(tokens[j], tokens[j + 1]):
                    following = tokens[j + 1]
                    if (following['surface'] in ('た', 'て', 'ば', 'でし')
                            and following['pos'][:1] in (['助動詞'], ['助詞'])):
                        continue
                yield first['span'][0], tokens[j]['span'][1]
                break
            if surface not in prefixes:
                break


RULE = GrammarRule(order=144,
    id='te_wa_ikenai', expression=TE_WA_IKENAI_GRAMMAR_EXPRESSION, reading='てはいけない',
    meaning='must not; may not; not allowed to',
    match=match_te_wa_ikenai,
)

NARANAI_RULE = GrammarRule(order=144,
    id='te_wa_naranai', expression='〜てはならない', reading='てはならない',
    meaning='must not; may not; not allowed to', match=match_te_wa_naranai,
)

RULES = (RULE, NARANAI_RULE)
