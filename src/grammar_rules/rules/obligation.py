"""Selected complete verbal obligation constructions."""
from src.GrammarConstants import OBLIGATION_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


# Precompute the complete forms and all prefixes once, rather than rebuilding
# the same strings for each token in every sentence.
_PREFIXES = {"なければ", "なきゃ", "なけりゃ", "なくては", "なくちゃ", "ないと"}
_ENDINGS = {"ならない", "なりません", "いけない", "いけません",
            "ならなかった", "いけなかった", "なりませんでした", "いけませんでした"}
_FORMS = frozenset(prefix + ending for prefix in _PREFIXES for ending in _ENDINGS)
_FORM_PREFIXES = frozenset(form[:i] for form in _FORMS for i in range(len(form) + 1))


def match_obligation(tokens):
    # Match whole aligned tokens only. Some dictionary analyses label complete
    # suffixes as nouns, so surfaces are checked alongside the verbal anchor.
    for i in range(1, len(tokens)):
        anchor = tokens[i - 1]
        pos = anchor['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith(('未然形', '連用形'))
                or not adjacent(anchor, tokens[i])):
            continue
        surface = ''
        for j in range(i, min(len(tokens), i + 10)):
            if j > i and not adjacent(tokens[j - 1], tokens[j]):
                break
            surface += tokens[j]['surface']
            if surface in _FORMS:
                # Do not reserve a partial inflection such as なかった.
                if j + 1 < len(tokens) and adjacent(tokens[j], tokens[j + 1]):
                    following = tokens[j + 1]
                    if following['surface'] == 'でし' and following['pos'][:1] == ['助動詞']:
                        continue
                    if following['surface'] in ('た', 'て', 'ば') and following['pos'][:1] in (['助動詞'], ['助詞']):
                        break
                yield tokens[i]['span'][0], tokens[j]['span'][1]
                break
            if surface not in _FORM_PREFIXES:
                break


RULE = GrammarRule(
    id='obligation', expression=OBLIGATION_GRAMMAR_EXPRESSION, reading='なければならない',
    meaning='must; have to; need to',
    match=match_obligation,
)
